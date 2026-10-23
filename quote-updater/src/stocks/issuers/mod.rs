//! One module per tokenized-stock design: its adapters to the ports (`crate::stocks::ports`), and the
//! kinds it registers, each a core pricer or guard (`crate::stocks`) over those adapters.

pub mod ondo;
