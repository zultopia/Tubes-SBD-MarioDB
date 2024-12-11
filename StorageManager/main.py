from StorageManager.classes import ConditionGroup, StorageManager, DataRetrieval, DataWrite, DataDeletion, Condition
from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager

if __name__ == "__main__":
    # frm = FailureRecoveryManager()
    manager = StorageManager(None)
    # print("Initial Data:", manager.data)

    # Write Example
    write_action = DataWrite(
        table="student",
        columns=["id", "name", "dept_name", "tot_cred"],
        new_values=[3, "Eve", "STEI", 101],
        level="row"
    )
    manager.write_block(write_action)
    manager.log_action("write", write_action.table, write_action.new_values, write_action.columns)
    # data = manager.read_block(DataRetrieval("Student", ["StudentID", "FullName", "GPA"], [], "", ""))
    # print("Data After Write:", data)

    # Read Example
    read_action = DataRetrieval(
        table="student",
        columns=["name", "tot_cred"],
        conditions=ConditionGroup([Condition("tot_cred", ">", 100)], "AND"),
        search_type="sequential",
        level="table"
    )
    results = manager.read_block(read_action)
    manager.log_action("read", read_action.table, results, read_action.columns)
    print("Read Results:", results)

    # Delete Example
    delete_action = DataDeletion(
        table="student",
        conditions=ConditionGroup([Condition("tot_cred", ">", 102)], "AND"),
        level="table"
    )
    # removed = manager.delete_block(delete_action)
    # manager.log_action("write", delete_action.table, {"deleted_rows": removed})
    # print("Removed Rows:", removed)
    # print("Data After Delete:", manager.data)
    manager.set_index("student", "name", "hash")
    manager.set_index("student", "tot_cred", "hash")
    
    data = manager.read_block_with_hash("student", "name", "Eve")
    print("Data", data)
    
    #removed = manager.delete_block(delete_action)
    #manager.log_action("write", delete_action.table, {"deleted_rows": removed})
    #print("Removed Rows:", removed)
    
    write_action = DataWrite(
        table="student",
        columns=["name", "tot_cred"],
        new_values=["EVA", 140],
        conditions=ConditionGroup([Condition("name", "=", "Eve")], logic_operator="AND"),
        level="row"
    )
    manager.write_block(write_action)
    
    results = manager.read_block(read_action)
    print("RESULT AFTER UPDATING", results)
    
    data = manager.read_block_with_hash("student", "name", "EVA")
    print("HASH AFTER UPDATING", data)
    
    # results = manager.delete_block(delete_action)
    # print("RESULTS DELETE BLOCK", results)
    
    read_hash_action = DataRetrieval(
        "student", ["id", "name"], 
        ConditionGroup([Condition("tot_cred", ">", 100)]), 
        "sequential", 
        "cell")
    
    read_hash_if = manager.read_block_with_hash("student", "tot_cred", 140)
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