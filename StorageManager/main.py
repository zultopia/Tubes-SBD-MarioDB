from StorageManager.classes import StorageManager, DataRetrieval, DataWrite, DataDeletion, Condition

if __name__ == "__main__":
    manager = StorageManager()
    # print("Initial Data:", manager.data)

    # Write Example
    write_action = DataWrite(
        table="Student",
        columns=["StudentID", "FullName", "GPA"],
        new_values=[3, "Eve", 3.9],
        level="row"
    )
    manager.write_block(write_action)
    manager.log_action("write", write_action.table, write_action.new_values, write_action.columns)
    # data = manager.read_block(DataRetrieval("Student", ["StudentID", "FullName", "GPA"], [], "", ""))
    # print("Data After Write:", data)

    # Read Example
    read_action = DataRetrieval(
        table="Student",
        columns=["FullName", "GPA"],
        conditions=[Condition("GPA", ">", 3.0)],
        search_type="sequential",
        level="table"
    )
    results = manager.read_block(read_action)
    manager.log_action("read", read_action.table, results, read_action.columns)
    print("Read Results:", results)

    # Delete Example
    delete_action = DataDeletion(
        table="Student",
        conditions=[Condition("StudentID", "=", 1)],
        level="table"
    )
    removed = manager.delete_block(delete_action)
    manager.log_action("write", delete_action.table, {"deleted_rows": removed})
    print("Removed Rows:", removed)
    # print("Data After Delete:", manager.data)
    manager.set_index("Student", "FullName", "hash")
    
    data = manager.read_block_with_hash("Student", "FullName", "Eve")
    print("Data", data)
    # Checking Logs
    print("\nAction Logs:")
    for log_entry in manager.action_logs:
        print(log_entry)