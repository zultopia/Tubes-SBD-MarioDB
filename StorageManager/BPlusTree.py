from typing import Union, List, Dict

class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children: List[Union[Dict, BPlusTreeNode]] = []

class BPlusTree:
    def __init__(self, degree):
        self.root = BPlusTreeNode(is_leaf=True)
        self.degree = degree

    def _find_leaf(self, node: BPlusTreeNode, key):
        if node.is_leaf:
            return node
        for i in range(len(node.keys)):
            if key < node.keys[i]:
                return self._find_leaf(node.children[i], key)
        return self._find_leaf(node.children[-1], key)

    def get(self, key):
        leaf = self._find_leaf(self.root, key)
        for i, item in enumerate(leaf.keys):
            if item == key:
                return leaf.children[i]
        return None

    def insert(self, key, value):
        leaf = self._find_leaf(self.root, key)
        if key in leaf.keys:
            raise ValueError(f"Key {key} tidak unik.")
        self._insert_in_leaf(leaf, key, value)
        if len(leaf.keys) == self.degree:
            self._split_node(leaf)

    def update(self, key, value):
        leaf = self._find_leaf(self.root, key)
        for i, item in enumerate(leaf.keys):
            if item == key:
                leaf.children[i] = value
                return
        raise KeyError(f"Key {key} tidak ditemukan.")

    def delete(self, key):
        leaf = self._find_leaf(self.root, key)
        if key in leaf.keys:
            index = leaf.keys.index(key)
            del leaf.keys[index]
            del leaf.children[index]
        else:
            raise KeyError(f"Key {key} tidak ditemukan.")

    def _insert_in_leaf(self, leaf: BPlusTreeNode, key, value):
        index = 0
        while index < len(leaf.keys) and key > leaf.keys[index]:
            index += 1
        leaf.keys.insert(index, key)
        leaf.children.insert(index, value)

    def _split_node(self, node: BPlusTreeNode):
        middle = len(node.keys) // 2
        sibling = BPlusTreeNode(is_leaf=node.is_leaf)
        sibling.keys = node.keys[middle:]
        sibling.children = node.children[middle:]
        node.keys = node.keys[:middle]
        node.children = node.children[:middle]

        if node == self.root:
            new_root = BPlusTreeNode()
            new_root.keys = [sibling.keys[0]]
            new_root.children = [node, sibling]
            self.root = new_root
        else:
            parent = self._find_parent(self.root, node)
            self._insert_in_internal(parent, sibling.keys[0], sibling)

    def _find_parent(self, current: BPlusTreeNode, child):
        if current.is_leaf or current.children[0].is_leaf:
            return None
        for node in current.children:
            if node == child:
                return current
            parent = self._find_parent(node, child)
            if parent:
                return parent
        return None

    def _insert_in_internal(self, parent: BPlusTreeNode, key, child):
        index = 0
        while index < len(parent.keys) and key > parent.keys[index]:
            index += 1
        parent.keys.insert(index, key)
        parent.children.insert(index + 1, child)
        if len(parent.keys) == self.degree:
            self._split_node(parent)