from bisect import bisect_left
from collections import deque
from itertools import product
from math import log, radians, sin, cos, atan2, sqrt
from numbers import Number


class RedBlackNode(object):
    RED = 0
    BLACK = 1

    @property
    def key(self):
        return self.__key

    @property
    def left(self):
        return self.__left

    @left.setter
    def left(self, left):
        if left is None or isinstance(left, RedBlackNode):
            self.__left = left

    @property
    def parent(self):
        return self.__parent

    @parent.setter
    def parent(self, parent):
        if parent is None or isinstance(parent, RedBlackNode):
            self.__parent = parent

    @property
    def right(self):
        return self.__right

    @right.setter
    def right(self, right):
        if right is None or isinstance(right, RedBlackNode):
            self.__right = right

    @property
    def grandparent(self):
        if self.__parent:
            return self.__parent.parent
        else:
            return None

    @property
    def great_uncle(self):
        grandparent = self.grandparent
        if grandparent:
            if grandparent.left == self.parent:
                return grandparent.right
            else:
                return grandparent.left
        else:
            return None

    @property
    def color(self):
        return self.__color

    @color.setter
    def color(self, color):
        if isinstance(color, int) and self.RED <= color <= self.BLACK:
            self.__color = color

    @property
    def values(self):
        if self.__ordered is None:
            return tuple().__iter__()
        else:
            return self.__ordered.__iter__()

    @property
    def ordered(self):
        if self.__ordered:
            return self.__ordered
        else:
            return []

    @property
    def attributes(self):
        if self.__attributes:
            return self.__attributes
        else:
            return {}

    __slots__ = ('__color', '__key', '__left', '__parent', '__right',
                 '__ordered', '__attributes', 'RED', 'BLACK')

    def __init__(self, key, parent=None, *order_attributes, **named_attributes):
        self.__color = self.RED
        self.__key = key
        self.__left = None
        self.__parent = parent
        self.__right = None

        if order_attributes:
            self.__ordered = list(order_attributes)
        else:
            self.__ordered = None

        if named_attributes:
            self.__attributes = dict(named_attributes)
        else:
            self.__attributes = None

    def __getattr__(self, attr):
        return self.attributes[attr]

    def __setattr__(self, attr, item):
        try:
            object.__setattr__(self, attr, item)
        except:
            if not self.__attributes:
                self.__attributes = {}
            self.__attributes[attr] = item

    def __getitem__(self, idx_or_key):
        if isinstance(idx_or_key, int):
            return self.ordered[idx_or_key]
        else:
            return self.attributes[idx_or_key]

    def __setitem__(self, idx_or_key, val):
        if isinstance(idx_or_key, int):
            if not self.__ordered:
                raise IndexError("list index out of range")
            self.__ordered[idx_or_key] = val
        else:
            if not self.__attributes:
                self.__attributes = {}
            self.__attributes[idx_or_key] = val

    def extend(self, iterator):
        if not self.__ordered:
            self.__ordered = []
        self.__ordered.extend(iterator)

    def update(self, mapping):
        if not self.__attributes:
            self.__attributes = {}
        self.__attributes.update(mapping)

    def __unicode__(self):
        return(u'<%d %s node(*(%s), **%s)>' % (
            self.__key, 'R' if self.__color == self.RED else 'B',
            u','.join(map(unicode, self.ordered)), repr(self.attributes)))

    def __repr__(self):
        return('<%d %s node(*(%s), **%s)>' % (
            self.__key, 'R' if self.__color == self.RED else 'B',
            ','.join(map(repr, self.ordered)), repr(self.attributes)))


class RedBlackTree(object):
    RED = RedBlackNode.RED
    BLACK = RedBlackNode.BLACK

    __slots__ = ('__root', '__size')

    @property
    def min(self):
        if self.__root:
            node = self.__root
            while node.left:
                node = node.left
            return node
        return None

    @property
    def max(self):
        if self.__root:
            node = self.__root
            while node.right:
                node = node.right
            return node
        return None

    def __init__(self):
        self.__root = None
        self.__size = 0

    def __len__(self):
        return self.__size

    def __repr__(self):
        return('<%s(%d) object at 0x%x>'
               % (self.__class__.__name__, len(self), id(self)))

    def __str__(self):
        current = None
        maxdigit = int(log(max(self.max.key, abs(self.min.key)), 10) + 1)
        maxheight = int(log(int(2 * log(len(self) + 1, 2)), 10) + 1)
        representation = ''
        for level, node in self.iter_breadth_first(self.__root, _level=0):
            if level != current:
                representation += ('\n%%0%dd ' % maxheight) % level
                current = level
            representation += ('%% %dd, ' % maxdigit) % node.key
        return representation

    def iter_sorted(self, reverse=False):
        pass

    @classmethod
    def __iter_breadth_first(cls, node, _level=None):
        if node:
            split_stack = [node]
            while split_stack:
                for _ in xrange(len(split_stack)):
                    node = split_stack.pop(0)
                    if isinstance(_level, int):
                        yield _level, node
                    else:
                        yield node
                    if node.left:
                        split_stack.append(node.left)
                    if node.right:
                        split_stack.append(node.right)
                if isinstance(_level, int):
                    _level += 1

    def iter_breadth_first(self, _level=None):
        return self.__iter_breadth_first(self.__root, _level)

    @classmethod
    def __iter_depth_first(cls, node, _level=None):
        if node:
            level = _level + 1 if isinstance(_level, int) else None
            for node_ in cls.__iter_depth_first(node.left, level):
                yield node_
            if isinstance(_level, int):
                yield _level, node
            else:
                yield node
            for node_ in cls.__iter_depth_first(node.right, level):
                yield node_

    def iter_depth_first(self, _level=None):
        return self.__iter_depth_first(self.__root, _level)

    def __iter__(self):
        return self.__iter_depth_first(self.__root)

    def insert(self, key, *order, **named):
        if self.__root:
            parent = self.__root
            while True:
                if parent.key == key:
                    if order:
                        parent.extend(order)
                    if named:
                        parent.update(named)
                    return

                if key < parent.key:
                    if parent.left:
                        parent = parent.left
                    else:
                        node = parent.left = RedBlackNode(
                            key, parent, *order, **named)
                        break
                else:
                    if parent.right:
                        parent = parent.right
                    else:
                        node = parent.right = RedBlackNode(
                            key, parent, *order, **named)
                        break
        else:
            parent = None
            node = self.__root = RedBlackNode(key, None, *order, **named)

        grandparent = node.grandparent
        great_uncle = node.great_uncle
        while True:
            if parent is None:
                node.color = self.BLACK
                break
            if parent.color == self.BLACK:
                break
            if great_uncle and great_uncle.color == self.RED:
                parent.color = great_uncle.color = self.BLACK
                grandparent.color = self.RED
                node = grandparent
                parent = node.parent
                grandparent = node.grandparent
                great_uncle = node.great_uncle
                continue

            if node == parent.right and parent == grandparent.left:
                grandparent.left = node
                node.parent = grandparent
                parent.right = node.left
                if node.left:
                    node.left.parent = parent
                node.left = parent
                parent.parent = node
                node = parent
                parent = node.parent
            elif node == parent.left and parent == grandparent.right:
                grandparent.right = node
                node.parent = grandparent
                parent.left = node.right
                if node.right:
                    node.right.parent = parent
                node.right = parent
                parent.parent = node
                node = parent
                parent = node.parent
            parent.color = self.BLACK
            grandparent.color = self.RED
            if node == parent.left:
                grandparent.left = parent.right
                if parent.right:
                    parent.right.parent = grandparent
                parent.right = grandparent
            else:
                grandparent.right = parent.left
                if parent.left:
                    parent.left.parent = grandparent
                parent.left = grandparent

            great_grandparent = grandparent.parent
            if great_grandparent:
                if grandparent == great_grandparent.left:
                    great_grandparent.left = parent
                else:
                    great_grandparent.right = parent
            else:
                self.__root = parent
                parent.color = self.BLACK
            parent.parent = great_grandparent
            grandparent.parent = parent
            break
        self.__size += 1

    def find_between(self, *keys):
        if self.__root:
            keys = filter(lambda val: isinstance(val, Number), keys)
            start = min(keys)
            until = max(keys)

            stack = [self.__root]
            while stack:
                node = stack.pop()
                if node.key >= start and node.key <= until:
                    yield node
                if node.right and node.key < until:
                    stack.append(node.right)
                if node.left and node.key > start:
                    stack.append(node.left)

    def find_nearest(self, key, fxn=lambda a, b: abs(a.key - b.key)):
        if self.__root and isinstance(key, Number):
            observed = deque()
            split_at = 0

            best = None
            stack = [self.__root]
            while stack:
                new = False
                node = stack.pop()
                if node.key == key:
                    best = node
                    break
                elif not best or abs(node.key - key) < abs(best.key - key):
                    new = True
                    best = node

                if node.right and node.key < key:
                    if new:
                        observed.appendleft(node)
                        split_at += 1
                    stack.append(node.right)
                elif node.left and node.key > key:
                    if new:
                        observed.append(node)
                    stack.append(node.left)

            yield best

            nodes = []
            dists = []
            total = 0

            for node in self.__iter_depth_first(self.__root):
                dist = fxn(node, best)
                if dist > 0:
                    index = bisect_left(dists, dist, lo=0, hi=total)
                    dists.insert(index, dist)
                    nodes.insert(index, node)
                    total += 1

            while nodes:
                yield nodes.pop(0)


class GeoTree(object):

    @classmethod
    def spawn_test_tree(cls, res=1):
        tree = cls()
        res = 1.0 / float(res)
        LAT = int(180 * res)
        LON = int(90 * res)
        for lat, lon in product(range(-LAT, LAT + 1, 1),
                                range(-LON, LON + 1, 1)):
            tree.insert(lat / res, lon / res)
        return tree

    def __init__(self):
        self.__tree = RedBlackTree()

    def __len__(self):
        return len(self.__tree)

    @staticmethod
    def __z_curve(x_coord, y_coord):
        bit = 1
        MAX = max(x_coord, y_coord)
        res = 0

        while bit <= MAX:
            bit <<= 1
        bit >>= 1

        while bit:
            res <<= 1
            if x_coord & bit:
                res |= 1
            res <<= 1
            if y_coord & bit:
                res |= 1
            bit >>= 1

        return res

    @classmethod
    def geo_curve(cls, latitude, longitude):
        return cls.__z_curve(int(round((latitude + 90.0) * 100000)),
                             int(round((longitude + 180.0) * 100000)))

    def insert(self, latitude, longitude, *order, **named):
        named.update({"latitude": latitude, "longitude": longitude})
        curve = self.geo_curve(latitude, longitude)
        self.__tree.insert(curve, *order, **named)

    def find_exactly(self, coords):
        curve = self.geo_curve(*coords)
        return self.__tree.find_between(curve)

    def find_between(self, min_coords, max_coords):
        min_curve = self.geo_curve(*min_coords)
        max_curve = self.geo_curve(*max_coords)

        min_lat = min(min_coords[0], max_coords[0])
        min_lon = min(min_coords[1], max_coords[1])
        max_lat = max(min_coords[0], max_coords[0])
        max_lon = max(min_coords[1], max_coords[1])

        for node in self.__tree.find_between(min_curve, max_curve):
            if (min_lat <= node.latitude and min_lon <= node.longitude and
                    max_lat >= node.latitude and max_lon >= node.longitude):
                yield node

    @staticmethod
    def haversine(coord1, coord2, radius=6371):
        latitude1, longitude1 = coord1
        latitude2, longitude2 = coord2

        diff_lat = radians(latitude2 - latitude1)
        diff_lon = radians(longitude2 - longitude1)

        sin_half_diff_lat = sin(diff_lat / 2)
        sin_half_diff_lon = sin(diff_lon / 2)

        A = (sin_half_diff_lat * sin_half_diff_lat + cos(radians(latitude1)) *
             sin_half_diff_lon * sin_half_diff_lon * cos(radians(latitude2)))
        return radius * 2 * atan2(sqrt(A), sqrt(1 - A))

    @classmethod
    def __haversine(cls, node1, node2):
        return cls.haversine((node1.latitude, node1.longitude),
                             (node2.latitude, node2.longitude))

    def find_nearest(self, coords):
        return self.__tree.find_nearest(key=self.geo_curve(*coords),
                                        fxn=self.__haversine)
