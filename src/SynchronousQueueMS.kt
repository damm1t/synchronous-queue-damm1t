import java.lang.Error
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

class SynchronousQueueMS<E> : SynchronousQueue<E> {

    private enum class NodeType { SENDER, GETTER }

    private class Node<T>(data: T?, val type: NodeType) {
        var cont: Continuation<Boolean>? = null
        var data = AtomicReference(data)
        var next: AtomicReference<Node<T>?> = AtomicReference(null)
    }

    private val head: AtomicReference<Node<E>>

    private val tail: AtomicReference<Node<E>>

    init {
        val dummy: Node<E> = Node(null, NodeType.SENDER)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    override suspend fun send(element: E) {
        val node = Node(element, NodeType.SENDER)
        while (true) {
            val curTail = tail.get()
            var curHead = head.get()
            if (curHead == curTail || curTail.type == NodeType.SENDER) {
                val next = curTail.next.get()
                if (curTail == tail.get()) {
                    if (next != null) {
                        tail.compareAndSet(curTail, next)
                        continue
                    }

                    val res = suspendCoroutine<Boolean> sc@{ cont ->
                        node.cont = cont
                        if (!curTail.next.compareAndSet(next, node)) {
                            cont.resume(false)
                            return@sc
                        }

                    }

                    if (res) {
                        this.tail.compareAndSet(curTail, node)
                        curHead = head.get()
                        if (node == curHead.next.get()) {
                            head.compareAndSet(curHead, node)
                        }
                        return
                    }
                }
            } else {
                val next = curHead.next.get()
                if (curTail != tail.get() || curHead != head.get() || next == null) {
                    continue
                }
                if (next.cont !== null && head.compareAndSet(curHead, next)) {
                    next.data.compareAndSet(null, element)
                    next.cont!!.resume(true)
                    return
                }
            }
        }
    }

    override suspend fun receive(): E {
        val node: Node<E> = Node(null, NodeType.GETTER)

        while (true) {
            val curTail = tail.get()
            var curHead = head.get()
            if (curHead == curTail || curTail.type == NodeType.GETTER) {
                val next = curTail.next.get()
                if (curTail == tail.get()) {
                    if (next != null) {
                        tail.compareAndSet(curTail, next)
                        continue
                    }

                    val res = suspendCoroutine<Boolean> sc@{ cont ->
                        node.cont = cont
                        if (!curTail.next.compareAndSet(next, node)) {
                            cont.resume(false)
                            return@sc
                        }

                    }

                    if (res) {
                        this.tail.compareAndSet(curTail, node)
                        curHead = head.get()
                        if (node == curHead.next.get()) {
                            head.compareAndSet(curHead, node)
                        }
                        return node.data.get()!!
                    }
                }
            } else {
                val next = curHead.next.get()
                if (curTail != tail.get() || curHead != head.get() || next == null) {
                    continue
                }
                val element = next.data.get() ?: continue
                if (next.cont !== null && head.compareAndSet(curHead, next)) {
                    next.data.compareAndSet(element, null)
                    next.cont!!.resume(true)
                    return element
                }
            }
        }
    }
}
