from typing import Union


class DoublyNode:
    """
    A class for creating a doubly linked list node

    Attributes
    ----------
    key : str
        The key of the node
    val : any
        The value of the node
    prev : DoublyNode
        The previous node
    next : DoublyNode
        The next node
    """

    def __init__(self, key: str, val: any):
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

    def _remove(self, node: DoublyNode):
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

    def _insert(self, node: DoublyNode):
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

    def get(self, key: str) -> Union[any, None]:
        """
        Gets the value of a key in the cache

        Parameters
        ----------
        key : str
            The key to be searched

        Returns
        -------
        any
            The value of the key
        """
        if key in self.cache:
            node = self.cache[key]
            self._remove(node)
            self._insert(node)
            return node.val
        return None

    def put(self, key: str, value: any):
        """
        Puts a key-value pair in the cache

        Parameters
        ----------
        key : str
            The key to be inserted
        value : any
            The value to be inserted
        """
        if key in self.cache:
            self._remove(self.cache[key])
        node = DoublyNode(key, value)
        self.cache[key] = node
        self._insert(node)

        if len(self.cache) > self.cap:
            node = self.oldest.next
            self._remove(node)
            del self.cache[node.key]

    def delete(self, key: str) -> bool:
        """
        Deletes a key from the cache

        Parameters
        ----------
        key : str
            The key to be deleted
        """
        if key in self.cache:
            node = self.cache[key]
            self._remove(node)
            del self.cache[key]
            return True
        return False

    def clear(self):
        """
        Clears the cache
        """
        # Clear the dictionary
        self.cache.clear()

        # Reinitialize the linked list
        self.oldest = DoublyNode(0, 0)
        self.latest = DoublyNode(0, 0)
        self.oldest.next = self.latest
        self.latest.prev = self.oldest
        self.latest.prev = self.oldest
