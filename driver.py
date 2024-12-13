from FailureRecoveryManager.Buffer import Buffer
from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager

buffer = Buffer(5)
buffer.put_buffer(
    "Student",
    1,
    [{"id": 1, "name": "John", "dept_name": "CS", "tot_cred": 12}],
)
buffer.put_buffer(
    "Student", 2, [{"id": 2, "name": "Suzy", "dept_name": "STEI", "tot_cred": None}]
)
buffer.put_buffer(
    "Student", 3, [{"id": 2, "name": "Suzy", "dept_name": "STEI", "tot_cred": None}]
)
buffer.put_buffer_hash(1, "Student", 4, "id", [{"id": 0}])
buffer.put_buffer_hash(2, "Student", 5, "name", [{"id": 0}])

frm = FailureRecoveryManager(
    buffer=buffer,
    checkpoint_interval=5,
    log_file="log.log",
)

while True:
    pass
