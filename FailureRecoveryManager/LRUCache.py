from typing import Union


class DoublyNode:
    """
    A class for creating a doubly linked list node

    Attributes
    ----------
    key : any
        The key of the node
    val : any
        The value of the node
    prev : DoublyNode
        The previous node
    next : DoublyNode
        The next node
    """

    def __init__(self, key: any, val: any):
        self.key = key
        self.val = val
        self.prev = None
        self.next = None


class LRUCache:
    """
    A class for creating a least recently used cache (used in failure recovery manager buffer)
    """

    def __init__(self, capacity: int):
        """
        Parameters
        ----------
        capacity : int
            The capacity of the cache
        """
        self.cap = capacity
        self.cache = {}

        self.oldest = DoublyNode(0, 0)
        self.latest = DoublyNode(0, 0)
        self.oldest.next = self.latest
        self.latest.prev = self.oldest

    def _remove(self, node: DoublyNode) -> None:
        """
        Removes a node from the linked list

        Parameters
        ----------
        node : DoublyNode
            The node to be removed
        """
        prev = node.prev
        next = node.next
        prev.next = next
        next.prev = prev

    def _insert(self, node: DoublyNode) -> None:
        """
        Inserts a node to the linked list

        Parameters
        ----------
        node : DoublyNode
            The node to be inserted
        """
        prev = self.latest.prev
        next = self.latest
        prev.next = next.prev = node
        node.next = next
        node.prev = prev

    def is_empty(self) -> bool:
        """
        Checks if the cache is empty

        Returns
        -------
        bool: True if the cache is empty, False otherwise
        """
        return len(self.cache) == 0

    def get(self, key: any) -> Union[any, None]:
        """
        Gets the value of a key in the cache

        Parameters
        ----------
        key : any
            The key to be searched

        Returns
        -------
        any: The value of the key
        None: If the key is not found
        """
        if key in self.cache:
            node = self.cache[key]
            self._remove(node)
            self._insert(node)
            return node.val
        return None

    def put(self, key: any, value: any) -> Union[any, None]:
        """
        Puts a key-value pair in the cache

        Parameters
        ----------
        key : any
            The key to be inserted
        value : any
            The value to be inserted

        Returns
        -------
        any: The least recently used value, overwritten by the new value
        None: There is still space in the cache or old key is overwritten
        """
        if key in self.cache:
            self._remove(self.cache[key])
        node = DoublyNode(key, value)
        self.cache[key] = node
        self._insert(node)

        if len(self.cache) > self.cap:
            # if full, must remove the oldest
            node = self.oldest.next
            self._remove(node)
            del self.cache[node.key]

            # return the least recently used value
            return node.val

        # if still have space or old key is overwritten
        return None

    def delete(self, key: any) -> bool:
        """
        Deletes a key from the cache

        Parameters
        ----------
        key : any
            The key to be deleted

        Returns
        -------
        bool: True if the key is deleted, False otherwise
        """
        if key in self.cache:
            node = self.cache[key]
            self._remove(node)
            del self.cache[key]
            return True
        return False

    def get_cache(self) -> dict[any, any]:
        """
        Gets the cache dictionary

        Returns
        -------
        dict[any, any]: The cache dictionary
        """
        newval = {}
        for key, node in self.cache.items():
            newval[key] = node.val
        return newval

    def clear(self) -> None:
        """
        Clears the cache

        Returns
        -------
        None
        """
        # Clear the current dictionary
        self.cache.clear()

        # Reinitialize the linked list
        self.oldest = DoublyNode(0, 0)
        self.latest = DoublyNode(0, 0)
        self.oldest.next = self.latest
        self.latest.prev = self.oldest
        self.latest.prev = self.oldest
