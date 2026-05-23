#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Partition(u32);

impl Partition {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u32 {
        self.0
    }
}

impl From<u32> for Partition {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl From<Partition> for u32 {
    fn from(p: Partition) -> Self {
        p.0
    }
}

impl From<i32> for Partition {
    fn from(v: i32) -> Self {
        Self(v as u32)
    }
}

impl From<Partition> for i32 {
    fn from(p: Partition) -> Self {
        p.0 as i32
    }
}

impl std::fmt::Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Offset(u64);

impl Offset {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Offset {
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<Offset> for u64 {
    fn from(o: Offset) -> Self {
        o.0
    }
}

impl From<i64> for Offset {
    fn from(v: i64) -> Self {
        Self(v as u64)
    }
}

impl From<Offset> for i64 {
    fn from(o: Offset) -> Self {
        o.0 as i64
    }
}

impl std::fmt::Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_new_and_value() {
        let p = Partition::new(42);
        assert_eq!(p.value(), 42);
    }

    #[test]
    fn partition_from_u32() {
        let p = Partition::from(5u32);
        assert_eq!(p.value(), 5);
    }

    #[test]
    fn partition_from_i32() {
        let p = Partition::from(7i32);
        assert_eq!(p.value(), 7);
    }

    #[test]
    fn partition_to_i32() {
        let p = Partition::new(7);
        let val: i32 = p.into();
        assert_eq!(val, 7);
    }

    #[test]
    fn partition_equality() {
        assert_eq!(Partition::new(1), Partition::new(1));
        assert_ne!(Partition::new(1), Partition::new(2));
    }

    #[test]
    fn offset_new_and_value() {
        let o = Offset::new(100);
        assert_eq!(o.value(), 100);
    }

    #[test]
    fn offset_from_i64() {
        let o = Offset::from(42i64);
        assert_eq!(o.value(), 42);
    }

    #[test]
    fn offset_to_i64() {
        let o = Offset::new(99);
        let val: i64 = o.into();
        assert_eq!(val, 99);
    }

    #[test]
    fn offset_ordering() {
        assert!(Offset::new(1) < Offset::new(2));
        assert!(Offset::new(5) > Offset::new(3));
    }
}
