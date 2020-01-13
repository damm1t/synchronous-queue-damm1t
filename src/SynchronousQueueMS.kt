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
            val curHead = head.get()
            if (curHead == curTail || curTail.type == NodeType.SENDER) {
                val res = suspendCoroutine<Boolean> sc@{ cont ->
                    node.cont = cont
                    if (curTail.next.compareAndSet(null, node)) {
                        tail.compareAndSet(curTail, node)
                    } else {
                        cont.resume(false)
                        return@sc
                    }
                }
                if (res) {
                    return
                }
            } else {
                val next = curHead.next.get()
                if (curTail != tail.get() || curHead != head.get() || curHead == tail.get() || next == null) {
                    continue
                }
                if (next.cont !== null && next.type == NodeType.GETTER && head.compareAndSet(curHead, next)) {
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
            val curHead = head.get()
            if (curHead == curTail || curTail.type == NodeType.GETTER) {
                val res = suspendCoroutine<Boolean> sc@{ cont ->
                    node.cont = cont
                    if (curTail.next.compareAndSet(null, node)) {
                        tail.compareAndSet(curTail, node)

                    } else {
                        cont.resume(false)
                        return@sc
                    }
                }
                if (res) {
                    return node.data.get()!!
                }
            } else {
                val next = curHead.next.get()
                if (curHead == tail.get() || curTail != tail.get() || curHead != head.get() || next == null) {
                    continue
                }
                val element = next.data.get() ?: continue
                if (next.cont !== null && next.type == NodeType.SENDER && head.compareAndSet(curHead, next)) {
                    next.data.compareAndSet(element, null)
                    next.cont!!.resume(true)
                    return element
                }
            }
        }
    }
}