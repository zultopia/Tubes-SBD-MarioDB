from threading import Lock
from typing import Union

from LRUCache import LRUCache


class Buffer:
    """
    BUFFER
    Stores a LRU Cache using a dictionary with a key as `<table_name>:<block_id>` and value as the block data.
    Easily implemented using doubly linked list and dictionary.
    """

    def __init__(self, capacity: int):
        """
        Parameters
        ----------
        capacity : int
            The number of blocks can be stored in the cache.
        """
        # Buffer max capacity
        self._capacity = capacity

        # Buffer cache instance
        self._buffer = LRUCache(capacity)

        # Buffer mutex
        self._lock = Lock()

    def get_buffer(self, table_name: str, block_id: int) -> Union[any, None]:
        """
        Get a block from the buffer cache

        Args:
            table_name (str): The name of the table
            block_id (int): The block ID

        Returns:
            any: The block data
            None: If the block is not found in the buffer
        """
        with self._buffer_lock:
            cache_key = (table_name, block_id)
            return self._buffer.get(cache_key)

    def put_buffer(
        self, table_name: str, block_id: int, block_data: any
    ) -> Union[any, None]:
        """
        Put a block to the buffer cache

        Args:
            table_name (str): The name of the table
            block_id (int): The block ID
            block_data (any): The block data

        Returns:
            None: if the buffer still has space or old block is overwritten
            Any: the block data that is overwritten
        """
        with self._buffer_lock:
            cache_key = (table_name, block_id)
            return self._buffer.put(cache_key, block_data)

    def delete_buffer(self, table_name: str, block_id: int) -> bool:
        """
        Delete a block from the buffer cache

        Args:
            table_name (str): The name of the table
            block_id (int): The block ID

        Returns:
            bool: True if the block is deleted, False if the block is not found
        """
        with self._buffer_lock:
            cache_key = (table_name, block_id)
            result = self._buffer.delete(cache_key)
            # if result:
            #     # print(
            #     #     f"[FRM | {str(datetime.now())}]: Block {block_id} of table {table_name} deleted from buffer."
            #     # )
            #     pass
            # else:
            #     # print(
            #     #     f"[FRM | {str(datetime.now())}]: Block {block_id} of table {table_name} not found in buffer."
            #     # )
            #     pass
            return result

    def get_buffer_values(self) -> dict[any, any]:
        """
        Get the buffer cache

        Returns:
        dict: The buffer cache
        """
        with self._buffer_lock:
            return self._buffer.get_cache()

    def clear_buffer(self) -> None:
        """
        Clear all the entries in the buffer cache

        Returns:
        previous buffer
        """
        with self._buffer_lock:
            self._buffer.clear()
            # print(f"[FRM | {str(datetime.now())}]: Buffer cleared.")
