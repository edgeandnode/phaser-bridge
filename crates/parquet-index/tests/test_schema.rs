use evm_common::transaction::TransactionRecord;
use typed_arrow::schema::SchemaMeta;

#[test]
fn print_schema() {
    let schema = TransactionRecord::schema();
    println!("\n\nTransactionRecord Schema:");
    for (i, field) in schema.fields().iter().enumerate() {
        println!(
            "{}: {} - {:?} (nullable: {})",
            i,
            field.name(),
            field.data_type(),
            field.is_nullable()
        );
    }
}
