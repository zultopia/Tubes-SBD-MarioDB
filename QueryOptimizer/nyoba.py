from StorageManager.classes import StorageManager, DataWrite, DataRetrieval, ConditionGroup
from FailureRecoveryManager.Buffer import Buffer


storage_manager = StorageManager(Buffer(100))
storage_manager.write_block_to_disk("Student", 0, [{
    "id": 1,
    "name": "Alice",
    "age": 20
}, {
    "id": 2,
    "name": "Bob",
    "age": 21
}, {
    "id": 3,
    "name": "Charlie",
    "age": 22
}, {
    "id": 4,
    "name": "David",
    "age": 23
}, {
    "id": 5,
    "name": "Eve",
    "age": 24
}])
print(storage_manager.read_block(DataRetrieval("Student", [
    "id",
    "dept_name"
], conditions=ConditionGroup([]), search_type="sequential", level="row")))
