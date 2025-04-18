use derive_traits::{FieldKeys as _, Keys};

#[test]
fn test_derive_keys() {
    #[derive(Debug, PartialEq, Eq, Keys)]
    struct Root {
        id: u32,
        child_a: Child,
        child_1: Child,
        name: String,
    }

    #[derive(Debug, PartialEq, Eq, Keys)]
    struct Child {
        id: u32,
        name: String,
    }

    assert_eq!(
        Root::field_keys(),
        vec![
            "ID",
            "CHILD_A.ID",
            "CHILD_A.NAME",
            "CHILD_1.ID",
            "CHILD_1.NAME",
            "NAME",
        ]
    );
}
