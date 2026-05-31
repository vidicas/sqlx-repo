use std::{
    fmt,
    ops::{Deref, DerefMut},
    str::FromStr,
};

use sqlx::{
    Decode, Encode,
    encode::IsNull,
    mysql::{MySql, MySqlTypeInfo, MySqlValueRef},
    postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef, Postgres},
    sqlite::{Sqlite, SqliteArgumentsBuffer, SqliteTypeInfo, SqliteValueRef},
    types::{Decimal as Inner, Type},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Decimal(pub Inner);

impl Deref for Decimal {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Decimal {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Inner> for Decimal {
    fn from(v: Inner) -> Self {
        Self(v)
    }
}

impl From<Decimal> for Inner {
    fn from(v: Decimal) -> Self {
        v.0
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for Decimal {
    type Err = <Inner as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Inner::from_str(s).map(Self)
    }
}

// SQLite: stored as TEXT (SQLite has no native decimal type)

impl Type<Sqlite> for Decimal {
    fn type_info() -> SqliteTypeInfo {
        <String as Type<Sqlite>>::type_info()
    }

    fn compatible(ty: &SqliteTypeInfo) -> bool {
        <String as Type<Sqlite>>::compatible(ty)
    }
}

impl Encode<'_, Sqlite> for Decimal {
    fn encode_by_ref(
        &self,
        buf: &mut SqliteArgumentsBuffer,
    ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <String as Encode<Sqlite>>::encode(self.0.to_string(), buf)
    }
}

impl<'r> Decode<'r, Sqlite> for Decimal {
    fn decode(value: SqliteValueRef<'r>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = <String as Decode<Sqlite>>::decode(value)?;
        Inner::from_str(&s).map(Self).map_err(Into::into)
    }
}

// Postgres: delegates to the inner Decimal impl (NUMERIC type)

impl Type<Postgres> for Decimal {
    fn type_info() -> PgTypeInfo {
        <Inner as Type<Postgres>>::type_info()
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        <Inner as Type<Postgres>>::compatible(ty)
    }
}

impl PgHasArrayType for Decimal {
    fn array_type_info() -> PgTypeInfo {
        <Inner as PgHasArrayType>::array_type_info()
    }
}

impl Encode<'_, Postgres> for Decimal {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <Inner as Encode<Postgres>>::encode_by_ref(&self.0, buf)
    }
}

impl<'r> Decode<'r, Postgres> for Decimal {
    fn decode(value: PgValueRef<'r>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        <Inner as Decode<Postgres>>::decode(value).map(Self)
    }
}

// MySQL: delegates to the inner Decimal impl (DECIMAL/NUMERIC type)

impl Type<MySql> for Decimal {
    fn type_info() -> MySqlTypeInfo {
        <Inner as Type<MySql>>::type_info()
    }

    fn compatible(ty: &MySqlTypeInfo) -> bool {
        <Inner as Type<MySql>>::compatible(ty)
    }
}

impl Encode<'_, MySql> for Decimal {
    fn encode_by_ref(
        &self,
        buf: &mut Vec<u8>,
    ) -> Result<IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <Inner as Encode<MySql>>::encode_by_ref(&self.0, buf)
    }
}

impl<'r> Decode<'r, MySql> for Decimal {
    fn decode(value: MySqlValueRef<'r>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        <Inner as Decode<MySql>>::decode(value).map(Self)
    }
}
