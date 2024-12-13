from typing import *

class JoinCondition:
    def __init__(self, column1: str, operation: str, column2: str):
        # Column in table1 (Left of JOIN)
        self.column1 = column1
        self.operation = operation
        #Column in table2 (right of JOIN)
        self.column2 = column2

class JoinProcessor:
    def natural_join(self, rows1: List[dict], rows2: List[dict], strategy: str="nested_loop"):
        """
        Perform a NATURAL JOIN between two tables using the inner join logic.
        Automatically matches columns with the same names. If no common columns, perform an inner join without condition.

        :param rows1: Rows from the first table.
        :param rows2: Rows from the second table.
        :param strategy: Strategy to use for the join ("nested_loop", "hash", "merge").
        :return: A list of joined rows.
        """
        # Identify common columns
        common_columns = set(rows1[0].keys()) & set(rows2[0].keys())

        if not common_columns:
            return self.join_on_nested_loop(rows1, rows2, None)

        result = rows1
        for col in common_columns:
            condition = JoinCondition(column1=col, operation="=", column2=col)
            if strategy == "nested_loop":
                result = self.join_on_nested_loop(result, rows2, condition)
            elif strategy == "hash":
                result = self.join_on_hash(result, rows2, condition)
            elif strategy == "merge":
                result = self.join_on_merge(result, rows2, condition)
            else:
                raise ValueError(f"Unsupported strategy: {strategy}")

        return result

    def join_on_nested_loop(self, rows1: List[dict], rows2: List[dict], condition: Union[JoinCondition, None], size: int = 10):
        """
        Perform a JOIN ON (inner join) using block nested loop strategy.
        Simulates processing rows in smaller blocks for memory optimization.
        
        :param rows1: Rows from the first table.
        :param rows2: Rows from the second table.
        :param condition: A Condition object specifying the join condition. If None, combines all rows.
        :param size: The number of rows in each block to simulate block processing.
        :return: A list of joined rows.
        """
        result = []

        # Process rows1 in blocks
        for i in range(0, len(rows1), size):
            block1 = rows1[i:i + size]

            for row1 in block1:
                for row2 in rows2:
                    if condition is None or self._evaluate_JoinCondition(row1, row2, condition):
                        result.append({**row1, **row2})

        return result


    def join_on_hash(self, rows1: List[dict], rows2: List[dict], condition: JoinCondition):
        """
        Perform a JOIN ON (inner join) using hash join strategy.

        :param rows1: Rows from the first table.
        :param rows2: Rows from the second table.
        :param condition: A Condition object specifying the join condition.
        :return: A list of joined rows.
        """
        result = []
        hash_table = {}

        for row in rows2:
            key = row.get(condition.column2)
            if key is None:
                raise KeyError(f"Key {condition.column2} not found in row: {row}")
            
            if key not in hash_table:
                hash_table[key] = []
            hash_table[key].append(row)
            
        for row1 in rows1:
            key = row1.get(condition.column1)
            if key is None:
                raise KeyError(f"Key {condition.column1} not found in row: {row1}")

            if key in hash_table:
                for row2 in hash_table[key]:
                    result.append({**row1, **row2})

        return result


    def join_on_merge(self, rows1: List[dict], rows2: List[dict], condition: JoinCondition):
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

    def _evaluate_JoinCondition(self, row1: dict, row2: dict, condition: JoinCondition):
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