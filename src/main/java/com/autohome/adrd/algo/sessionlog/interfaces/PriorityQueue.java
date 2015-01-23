package com.autohome.adrd.algo.sessionlog.interfaces;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */
public class PriorityQueue {

	private int size = 0;
	//private ProcessorEntry[] queue = new ProcessorEntry[256];
	public ProcessorEntry[] queue = new ProcessorEntry[256];

	public PriorityQueue() {
	}

	public byte[] getBytes() {
		return null;
	}

	public int size() {
		return size;
	}

	public void add(ProcessorEntry entry) {
		if (size + 1 == queue.length)
			queue = Arrays.copyOf(queue, 2 * queue.length);
		queue[++size] = entry;
		fixUp(size);
	}

	public ProcessorEntry getMin() {
		return queue[1];
	}

	public void removeMin() {
		queue[1] = queue[size];
		queue[size--] = null;
		fixDown(1);
	}

	public void quickRemove(int i) {
		assert i <= size;
		queue[i] = queue[size];
		queue[size--] = null;
	}

	public boolean isEmpty() {
		return size == 0;
	}

	public void clear() {
		for (int i = 1; i <= size; i++)
			queue[i] = null;
		size = 0;
	}
	
	private void fixUp(int k) {
        while (k > 1) {
            int j = k >> 1;
            if (queue[j].less(queue[k]) || queue[j].equals(queue[k])) break;
            //ProcessorEntry tmp = queue[j];  queue[j] = queue[k]; queue[k] = tmp;
            ProcessorEntry tmp = queue[j];  
            queue[j] = queue[k]; 
            queue[k] = tmp;
            k = j;
        }
    }

    private void fixDown(int k) {
        int j;
        while ((j = k << 1) <= size && j > 0) {
            if (j < size && queue[j].greater(queue[j+1])) j++;
            if (queue[k].less(queue[j]) || queue[k].equals(queue[j])) break;
            ProcessorEntry tmp = queue[j];  queue[j] = queue[k]; queue[k] = tmp;
            k = j;
        }
    }
    
	public void heapify() {
		for (int i = size / 2; i >= 1; i--)
			fixDown(i);
	}
	
	public Iterator<ProcessorEntry> iterator() {
		return new Iterator<ProcessorEntry>() {

			public boolean hasNext() {
				if (PriorityQueue.this.isEmpty())
					return false;
				return true;
			}

			public ProcessorEntry next() {
				ProcessorEntry entry = PriorityQueue.this.getMin();
				PriorityQueue.this.removeMin();
				return entry;
			}

			public void remove() {
				throw new RuntimeException("Not supported operation");
			}
		};
	}

}
