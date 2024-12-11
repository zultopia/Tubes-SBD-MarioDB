from StorageManager.classes import ConditionGroup, StorageManager, DataRetrieval, DataWrite, DataDeletion, Condition
from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager

if __name__ == "__main__":
    # frm = FailureRecoveryManager()
    manager = StorageManager(None)
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
        conditions=ConditionGroup([Condition("GPA", ">", 3.0)], "AND"),
        search_type="sequential",
        level="table"
    )
    results = manager.read_block(read_action)
    manager.log_action("read", read_action.table, results, read_action.columns)
    print("Read Results:", results)

    # Delete Example
    delete_action = DataDeletion(
        table="Student",
        conditions=ConditionGroup([Condition("GPA", ">", 2.0)], "AND"),
        level="table"
    )
    # removed = manager.delete_block(delete_action)
    # manager.log_action("write", delete_action.table, {"deleted_rows": removed})
    # print("Removed Rows:", removed)
    # print("Data After Delete:", manager.data)
    manager.set_index("Student", "FullName", "hash")
    manager.set_index("Student", "GPA", "hash")
    
    data = manager.read_block_with_hash("Student", "FullName", "Eve")
    print("Data", data)
    
    #removed = manager.delete_block(delete_action)
    #manager.log_action("write", delete_action.table, {"deleted_rows": removed})
    #print("Removed Rows:", removed)
    
    write_action = DataWrite(
        table="Student",
        columns=["FullName", "GPA"],
        new_values=["EVA", 2.1],
        conditions=ConditionGroup([Condition("FullName", "=", "Eve")], logic_operator="AND"),
        level="row"
    )
    manager.write_block(write_action)
    
    results = manager.read_block(read_action)
    print("RESULT AFTER UPDATING", results)
    
    data = manager.read_block_with_hash("Student", "FullName", "EVA")
    print("HASH AFTER UPDATING", data)
    
    # results = manager.delete_block(delete_action)
    # print("RESULTS DELETE BLOCK", results)
    
    read_hash_action = DataRetrieval(
        "Student", ["StudentID", "FullName"], 
        ConditionGroup([Condition("GPA", ">", 1.0)]), 
        "sequential", 
        "cell")
    
    read_hash_if = manager.read_block_with_hash("Student", "GPA", 2.1)
    print("READ HASH WITH NUMBER", read_hash_if)
    
    test_stat = manager.get_stats()
    print("TEST STAT", test_stat)
    
    all_relation = manager.get_all_relations()
    print(all_relation)
    
    for relation in all_relation:
        print(relation, ":")
        print(manager.get_all_attributes(relation))
        print()
    
    # Checking Logs
    print("\nAction Logs:")
    for log_entry in manager.action_logs:
        print(log_entry)