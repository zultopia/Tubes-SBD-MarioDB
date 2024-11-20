from StorageManager.classes import StorageManager, DataRetrieval, DataWrite, DataDeletion, Condition

if __name__ == "__main__":
    manager = StorageManager()
    print("Initial Data:", manager.data)
    
    # Contoh
    # Write
    manager.write_block(DataWrite("Student", ["StudentID", "FullName", "GPA"], [1, "Charlie", 3.2]))
    print("Data After Write:", manager.data)
    
    # Read
    results = manager.read_block(DataRetrieval("Student", ["FullName", "GPA"], [Condition("GPA", ">", 3.0)]))
    print("Read Results:", results)
    
    # Delete
    removed = manager.delete_block(DataDeletion("Student", [Condition("StudentID", "=", 1)]))
    print("Removed Rows:", removed)
    print("Data After Delete:", manager.data)