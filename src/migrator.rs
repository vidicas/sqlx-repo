use crate::Result;

pub struct Migrator {}

impl Migrator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn add_migration(&mut self, query: &str) -> Result<()> {
        Ok(())
    }
}
