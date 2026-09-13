//! Per-table configuration for incremental sync: which column to filter on, and which
//! column (or columns) identify a row for `MERGE`.
//!
//! Pure data. Nothing here touches SQL Server or Delta, so it is testable without either.
//!
//! A table absent from a [`SyncCatalog`] is not synced: this crate never guesses which
//! tables exist or which columns are suitable, since across an unfamiliar third-party
//! schema that guess is exactly the kind of silent assumption pgdelta's own design
//! philosophy avoids. Every table synced is one the caller named on purpose.

use crate::error::{Error, MissingConfig, Result};

/// How one table is kept in sync incrementally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSync {
    /// Qualified table name, exactly as the source database names it.
    pub table: String,
    /// Column filtered on: `WHERE <watermark_column> > <last_synced_value>`. Must be
    /// monotonically non-decreasing with insert/update order (a timestamp column
    /// updated on every write, or a strictly increasing id) for incremental sync to be
    /// correct; this crate has no way to verify that property itself and trusts the
    /// caller's configuration.
    pub watermark_column: String,
    /// Column, or columns, `MERGE` matches existing rows on. Almost always the table's
    /// actual primary key; more than one column supports a composite key.
    pub primary_key: Vec<String>,
}

impl TableSync {
    /// Validates that a [`TableSync`] is usable: non-empty table name, watermark
    /// column, and at least one primary key column.
    ///
    /// This is a configuration check, not a connectivity one: it says nothing about
    /// whether the named columns actually exist in the source, which can only be
    /// confirmed once a connection is open.
    ///
    /// # Errors
    ///
    /// [`Error::IncrementalConfigMissing`] if the watermark column or every primary key
    /// column is empty or blank.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn validate(&self) -> Result<()> {
        if self.watermark_column.trim().is_empty() {
            return Err(Error::IncrementalConfigMissing {
                table: self.table.clone(),
                missing: MissingConfig::WatermarkColumn,
            });
        }
        if self.primary_key.is_empty() || self.primary_key.iter().all(|c| c.trim().is_empty()) {
            return Err(Error::IncrementalConfigMissing {
                table: self.table.clone(),
                missing: MissingConfig::PrimaryKey,
            });
        }
        Ok(())
    }
}

/// The set of tables one sync run covers, and how each is kept in sync.
///
/// Construct with [`SyncCatalog::new`], which validates every entry up front: a
/// misconfigured table is rejected before any connection is opened or any query run,
/// rather than failing partway through a run that already touched other tables.
#[derive(Debug, Clone, Default)]
pub struct SyncCatalog {
    tables: Vec<TableSync>,
}

impl SyncCatalog {
    /// Builds a catalog from `tables`, validating each entry.
    ///
    /// # Errors
    ///
    /// [`Error::IncrementalConfigMissing`] if any table is missing its watermark column
    /// or primary key, and [`Error::Internal`] if the same table name is configured more
    /// than once (which one would silently win is not a decision this crate makes for
    /// the caller).
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn new(tables: Vec<TableSync>) -> Result<Self> {
        let mut seen = std::collections::HashSet::with_capacity(tables.len());
        for t in &tables {
            t.validate()?;
            if !seen.insert(t.table.clone()) {
                return Err(Error::Internal {
                    detail: "a table was configured more than once in the sync catalog",
                });
            }
        }
        Ok(Self { tables })
    }

    /// Tables covered by this run, in the order they were configured.
    pub fn tables(&self) -> &[TableSync] {
        &self.tables
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid(table: &str) -> TableSync {
        TableSync {
            table: table.to_string(),
            watermark_column: "updated_at".to_string(),
            primary_key: vec!["id".to_string()],
        }
    }

    #[test]
    fn a_well_formed_catalog_is_accepted() {
        let catalog =
            SyncCatalog::new(vec![valid("dbo.customers"), valid("dbo.invoices")]).unwrap();
        assert_eq!(catalog.tables().len(), 2);
    }

    #[test]
    fn a_missing_watermark_column_is_rejected() {
        let mut t = valid("dbo.customers");
        t.watermark_column = String::new();
        let err = SyncCatalog::new(vec![t]).unwrap_err();
        assert!(matches!(
            err,
            Error::IncrementalConfigMissing {
                missing: MissingConfig::WatermarkColumn,
                ..
            }
        ));
    }

    #[test]
    fn a_blank_watermark_column_is_rejected() {
        let mut t = valid("dbo.customers");
        t.watermark_column = "   ".to_string();
        assert!(SyncCatalog::new(vec![t]).is_err());
    }

    #[test]
    fn an_empty_primary_key_is_rejected() {
        let mut t = valid("dbo.customers");
        t.primary_key = Vec::new();
        let err = SyncCatalog::new(vec![t]).unwrap_err();
        assert!(matches!(
            err,
            Error::IncrementalConfigMissing {
                missing: MissingConfig::PrimaryKey,
                ..
            }
        ));
    }

    #[test]
    fn a_primary_key_of_only_blank_columns_is_rejected() {
        let mut t = valid("dbo.customers");
        t.primary_key = vec!["  ".to_string(), "".to_string()];
        assert!(SyncCatalog::new(vec![t]).is_err());
    }

    #[test]
    fn a_composite_primary_key_is_accepted() {
        let mut t = valid("dbo.order_items");
        t.primary_key = vec!["order_id".to_string(), "line_no".to_string()];
        let catalog = SyncCatalog::new(vec![t]).unwrap();
        assert_eq!(catalog.tables()[0].primary_key.len(), 2);
    }

    #[test]
    fn duplicate_table_names_are_rejected() {
        let err =
            SyncCatalog::new(vec![valid("dbo.customers"), valid("dbo.customers")]).unwrap_err();
        assert!(matches!(err, Error::Internal { .. }));
    }

    #[test]
    fn an_empty_catalog_is_allowed() {
        // Not an error: a caller narrowing to zero tables (all filtered out upstream,
        // say) should get an empty, well-formed catalog rather than a spurious failure.
        let catalog = SyncCatalog::new(vec![]).unwrap();
        assert!(catalog.tables().is_empty());
    }
}
