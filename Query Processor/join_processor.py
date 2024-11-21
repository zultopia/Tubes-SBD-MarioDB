from typing import *

class Condition:
    def __init__(self, column1: str, operation: str, column2: str):
        self.column1 = column1
        self.operation = operation
        self.column2 = column2

class JoinProcessor:
    def process_join(self, data: dict, table1: str, table2: str, join_type: str, 
                     condition: Union[Condition, None] = None, strategy: str = "block_nested_loop"):
        """
        Perform JOIN ON and NATURAL JOIN operations between two tables using a specified strategy.
        Assume if strategy is merge or hash, there is condition.

        :param data: Dictionary containing table names as keys and rows as values.
        :param table1: Name of the first table.
        :param table2: Name of the second table.
        :param join_type: Type of join ("default" (inner), "natural").
        :param condition: A Condition object for JOIN ON (ignored for NATURAL JOIN).
        :param strategy: Strategy to use for the join ("block_nested_loop", "hash", "merge").
        :return: A list of joined rows.
        """
        rows1 = data.get(table1, [])
        rows2 = data.get(table2, [])
        if not rows1 or not rows2:
            raise ValueError(f"One or both tables ('{table1}', '{table2}') do not exist or are empty.")

        if join_type == "natural":
            return self.natural_join(rows1, rows2, strategy)

        elif join_type == "default":  # INNER JOIN
            if not condition:
                raise ValueError("JOIN ON requires a condition.")
            
            if strategy == "block_nested_loop":
                return self.join_on_block_nested_loop(rows1, rows2, condition)
            elif strategy == "hash":
                return self.join_on_hash(rows1, rows2, condition)
            elif strategy == "merge":
                return self.join_on_merge(rows1, rows2, condition)
            else:
                raise ValueError(f"Unsupported strategy: {strategy}")

        else:
            raise ValueError(f"Unsupported join type: {join_type}")

    def natural_join(self, rows1: List[dict], rows2: List[dict], strategy: str):
        """
        Perform a NATURAL JOIN between two tables using the inner join logic.
        Automatically matches columns with the same names. If no common columns, perform an inner join without condition.

        :param rows1: Rows from the first table.
        :param rows2: Rows from the second table.
        :param strategy: Strategy to use for the join ("block_nested_loop", "hash", "merge").
        :return: A list of joined rows.
        """
        # Identify common columns
        common_columns = set(rows1[0].keys()) & set(rows2[0].keys())

        if not common_columns:
            return self.join_on_block_nested_loop(rows1, rows2, None)

        result = rows1
        for col in common_columns:
            condition = Condition(column1=col, operation="=", column2=col)
            if strategy == "block_nested_loop":
                result = self.join_on_block_nested_loop(result, rows2, condition)
            elif strategy == "hash":
                result = self.join_on_hash(result, rows2, condition)
            elif strategy == "merge":
                result = self.join_on_merge(result, rows2, condition)
            else:
                raise ValueError(f"Unsupported strategy: {strategy}")

        return result



    def join_on_block_nested_loop(self, rows1: List[dict], rows2: List[dict], condition: Union[Condition, None], block_size: int = 10):
        """
        Perform a JOIN ON (inner join) using block nested loop strategy.
        Simulates processing rows in smaller blocks for memory optimization.
        
        :param rows1: Rows from the first table.
        :param rows2: Rows from the second table.
        :param condition: A Condition object specifying the join condition. If None, combines all rows.
        :param block_size: The number of rows in each block to simulate block processing.
        :return: A list of joined rows.
        """
        result = []

        # Process rows1 in blocks
        for i in range(0, len(rows1), block_size):
            block1 = rows1[i:i + block_size]

            # Compare each row in the block with all rows in rows2
            for row1 in block1:
                for row2 in rows2:
                    if condition is None or self._evaluate_condition(row1, row2, condition):
                        result.append({**row1, **row2})

        return result


    def join_on_hash(self, rows1: List[dict], rows2: List[dict], condition: Condition):
        """
        Perform a JOIN ON (inner join) using hash join strategy.

        :param rows1: Rows from the first table.
        :param rows2: Rows from the second table.
        :param condition: A Condition object specifying the join condition.
        :return: A list of joined rows.
        """
        result = []
        hash_table = {}

        # Build hash table for rows2
        for row in rows2:
            key = row.get(condition.column2)
            if key is None:
                raise KeyError(f"Key {condition.column2} not found in row: {row}")
            
            if key not in hash_table:
                hash_table[key] = []
            hash_table[key].append(row)

        # Probe hash table with rows1
        for row1 in rows1:
            key = row1.get(condition.column1)
            if key is None:
                raise KeyError(f"Key {condition.column1} not found in row: {row1}")

            if key in hash_table:
                for row2 in hash_table[key]:
                    result.append({**row1, **row2})

        return result


    def join_on_merge(self, rows1: List[dict], rows2: List[dict], condition: Condition):
        """
        Perform a JOIN ON (inner join) using merge join strategy.
        Sort both rows1 and rows2 on the join keys.

        :param rows1: Rows from the first table.
        :param rows2: Rows from the second table.
        :param condition: A Condition object specifying the join condition.
        :return: A list of joined rows.
        """
        rows1 = sorted(rows1, key=lambda row: row[condition.column1])
        rows2 = sorted(rows2, key=lambda row: row[condition.column2])

        result = []
        i, j = 0, 0

        while i < len(rows1) and j < len(rows2):
            key1 = rows1[i][condition.column1]
            key2 = rows2[j][condition.column2]

            if key1 == key2:
                result.append({**rows1[i], **rows2[j]})
                j += 1
            elif key1 < key2:
                i += 1
            else:
                j += 1

        return result

    def _evaluate_condition(self, row1: dict, row2: dict, condition: Condition):
        """
        Evaluate the JOIN ON condition between two rows.
        """
        value1 = row1.get(condition.column1)
        value2 = row2.get(condition.column2)
        operation = condition.operation

        return {
            "=": value1 == value2,
            "<>": value1 != value2,
            ">": value1 > value2,
            ">=": value1 >= value2,
            "<": value1 < value2,
            "<=": value1 <= value2,
        }[operation]

# Tests with assert

data = {
    "Users": [
        {"UserID": 1, "Name": "Alice", "Age": 25},
        {"UserID": 2, "Name": "Bob", "Age": 30},
        {"UserID": 3, "Name": "Charlie", "Age": 35},
    ],
    "Orders": [
        {"OrderID": 101, "UserID": 1, "Amount": 250.0},
        {"OrderID": 102, "UserID": 2, "Amount": 150.0},
        {"OrderID": 103, "UserID": 4, "Amount": 300.0},
    ],
}

condition = Condition(column1="UserID", operation="=", column2="UserID")
processor = JoinProcessor()

# Block Nested Loop Join
result_block_nested = processor.process_join(data, "Users", "Orders", "default", condition, strategy="block_nested_loop")
assert result_block_nested == [
    {"UserID": 1, "Name": "Alice", "Age": 25, "OrderID": 101, "Amount": 250.0},
    {"UserID": 2, "Name": "Bob", "Age": 30, "OrderID": 102, "Amount": 150.0},
]

# Hash Join
result_hash = processor.process_join(data, "Users", "Orders", "default", condition, strategy="hash")
assert result_hash == [
    {"UserID": 1, "Name": "Alice", "Age": 25, "OrderID": 101, "Amount": 250.0},
    {"UserID": 2, "Name": "Bob", "Age": 30, "OrderID": 102, "Amount": 150.0},
]

# Merge Join
result_merge = processor.process_join(data, "Users", "Orders", "default", condition, strategy="merge")
assert result_merge == [
    {"UserID": 1, "Name": "Alice", "Age": 25, "OrderID": 101, "Amount": 250.0},
    {"UserID": 2, "Name": "Bob", "Age": 30, "OrderID": 102, "Amount": 150.0},
]

# Natural Join
result_natural = processor.process_join(data, "Users", "Orders", "natural", strategy="block_nested_loop")
assert result_natural == [
    {"UserID": 1, "Name": "Alice", "Age": 25, "OrderID": 101, "Amount": 250.0},
    {"UserID": 2, "Name": "Bob", "Age": 30, "OrderID": 102, "Amount": 150.0},
]

print("All tests passed!")