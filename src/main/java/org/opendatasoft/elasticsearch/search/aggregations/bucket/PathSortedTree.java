package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Stack;

public class PathSortedTree<K, T> implements Iterable<T> {

    private Comparator<? super T> comparator;
    private Node<K, T> root;
    private int size = -1;
    private int fullSize = 0;

    public PathSortedTree(Comparator<? super T> comparator) {
        root = new Node<>(comparator);
        this.comparator = comparator;
    }

    public PathSortedTree(Comparator<? super T> comparator, int size) {
        this(comparator);
        this.size = size;
    }

    public int getFullSize() {
        return fullSize;
    }

    public void add(K[] path, T element) {
        /* Please note that paths in path must be descending-sorted by level. */
        Node<K, T> currentNode = root;
        for (K k : path) {
            boolean newChild = true;
            for (Node<K, T> child : currentNode.children) {
                if (child.key.equals(k)) {
                    currentNode = child;
                    newChild = false;
                    break;
                }
            }
            if (newChild) {
                Node<K, T> newNode = new Node<>(k, comparator, element, currentNode);
                currentNode.children.add(newNode);
                fullSize++;
                break;
            }
        }
    }

    public List<T> getAsList() {

        List<T> result = new ArrayList<>(fullSize);

        Iterator<T> iterator = consumer();

        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    public Iterator<T> consumer() {
        return new PathSortedTreeConsumer(root, fullSize);
    }

    @Override
    public Iterator<T> iterator() {
        return new PathSortedTreeIterator(root);
    }

    public static class Node<K, T> {
        private K key;
        private T data;
        private Node<K, T> parent;

        private PriorityQueue<Node<K, T>> children;

        Node() {
            this.children = new PriorityQueue<>();
        }

        public Node(Comparator<? super T> comparator) {
            this.children = new PriorityQueue<>(getComparator(comparator));
        }

        Comparator<Node<K, T>> getComparator(Comparator<? super T> comparator) {
            return (n1, n2) -> comparator.compare(n1.data, n2.data);
        }

        public Node(K key, Comparator<? super T> comparator, T data, Node<K, T> parent) {
            this.key = key;
            this.data = data;
            this.children = new PriorityQueue<>(getComparator(comparator));
            this.parent = parent;
        }
    }

    private class PathSortedTreeIterator implements Iterator<T> {

        private Stack<Iterator<Node<K, T>>> iterators;
        Iterator<Node<K, T>> current;

        PathSortedTreeIterator(Node<K, T> root) {
            current = root.children.iterator();
            iterators = new Stack<>();
        }

        @Override
        public boolean hasNext() {
            return current.hasNext();
        }

        @Override
        public T next() {

            Node<K, T> nextNode = current.next();

            if (!nextNode.children.isEmpty()) {
                iterators.push(current);
                current = nextNode.children.iterator();
            } else if (!current.hasNext()) {
                while (!iterators.empty()) {
                    current = iterators.pop();
                    if (current.hasNext()) {
                        break;
                    }
                }
            }

            return nextNode.data;

        }
    }

    private class PathSortedTreeConsumer implements Iterator<T> {

        Node<K, T> cursor;

        int currentSize = 0;
        int iteratorFullSize;

        PathSortedTreeConsumer(Node<K, T> root, int fullSize) {
            iteratorFullSize = fullSize;
            cursor = root;
        }

        @Override
        public boolean hasNext() {
            if (size >= 0 && currentSize >= size) {
                return false;
            }
            if (cursor.children.size() > 0) {
                return true;
            }

            return currentSize < iteratorFullSize;
        }

        @Override
        public T next() {

            Node<K, T> nextNode = null;
            while (nextNode == null) {
                nextNode = cursor.children.poll();
                if (nextNode == null) {
                    if (cursor.parent == null) {
                        break;
                    }
                    cursor = cursor.parent;
                }
            }
            if (nextNode == null) throw new NoSuchElementException();
            currentSize++;
            fullSize--;
            cursor = nextNode;
            return nextNode.data;

        }
    }

}
