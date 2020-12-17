package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    // Whether or not we can enable or disable auto escalate
    public boolean isTableContext;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
        this.isTableContext = false;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly) {throw new UnsupportedOperationException("Acquire: Context is readonly");}
        if (name.parent()!=null) {
            LockType p_type = lockman.getLockType(transaction, name.parent());
            if (!LockType.canBeParentLock(p_type, lockType)) {
                throw new InvalidLockException("Acquire: Request is invalid.");
            }
        }
        lockman.acquire(transaction, name, lockType);
        if (name.parent()!=null) {
            int num = parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
            parent.numChildLocks.put(transaction.getTransNum(), num+1);
        }
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {throw new UnsupportedOperationException("Release: Context is readonly");}
        if (numChildLocks.getOrDefault(transaction.getTransNum(), 0) > 0) {
            throw new InvalidLockException("Release: Lock cannot be released");
        }
        lockman.release(transaction, name);
        if (parent!=null) {
            int num = parent.numChildLocks.get(transaction.getTransNum());
            parent.numChildLocks.put(transaction.getTransNum(), num-1);
        }
    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {throw new UnsupportedOperationException("Promote: Context is readonly");}
        List<ResourceName> sis = sisDescendants(transaction);
        sis.add(name);
        if (newLockType == LockType.SIX) {
            if (hasSIXAncestor(transaction)) {throw new InvalidLockException("Promote: Has six ancestor and is six");}
            LockType check_type = lockman.getLockType(transaction, name);
            if (!((check_type==LockType.IS) || (check_type==LockType.IX) || (check_type==LockType.S))) {
                throw new InvalidLockException("Promote: not correct type for six");
            }
            lockman.acquireAndRelease(transaction, name, newLockType, sis);
            if (parent!=null) {
                int num = parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
                parent.numChildLocks.put(transaction.getTransNum(), num-sis.size());
            }
            return;
        }
        if (getExplicitLockType(transaction)==LockType.IS && newLockType==LockType.S) {
            lockman.acquireAndRelease(transaction, name, newLockType, sis);
            if (parent!=null) {
                int num = parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
                parent.numChildLocks.put(transaction.getTransNum(), num-sis.size());
            }
            return;
        }
        if (parent!=null) {
            if (!LockType.canBeParentLock(lockman.getLockType(transaction, parent.name), newLockType)) {
                throw new InvalidLockException("Promote: cant be a parent");
            }
        }
        lockman.promote(transaction, name, newLockType);
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) {throw new UnsupportedOperationException("Escalate: Context is readonly");}
        if (lockman.getLockType(transaction, name) == LockType.NL) {throw new NoLockHeldException("Escalate: No Lock");}
        List<ResourceName> release = new ArrayList<>();
        release.add(name);
        LockType type = lockman.getLockType(transaction, name);
        LockType temp_type = LockType.S;
        for (Lock l : lockman.getLocks(transaction)) {if (l.name.isDescendantOf(name)) {release.add(l.name);}}
        if ((type!=LockType.S) && (type!=LockType.IS)) {temp_type = LockType.X;}
        if ((temp_type==type) && (release.size()==1)) {return;}
        lockman.acquireAndRelease(transaction, name, temp_type, release);
        numChildLocks.remove(transaction.getTransNum());
        for (ResourceName release_name : release) {
            if (!release_name.equals(name)) {
                LockContext release_context = fromResourceName(lockman, release_name);
                release_context.numChildLocks.remove(transaction.getTransNum());
            }
        }
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {return LockType.NL;}
        // TODO(proj4_part2): implement
        LockType eff_lock = lockman.getLockType(transaction, name);
        if (eff_lock != LockType.NL && eff_lock != LockType.IX) {return eff_lock;}
        LockContext top = parentContext();
        while (top!=null) {
            if (top.lockman.getLockType(transaction, top.name) == LockType.SIX && eff_lock==LockType.IX) {
                return LockType.SIX;
            }
            if (top.lockman.getLockType(transaction, top.name) == LockType.SIX && eff_lock!=LockType.IX) {
                return LockType.S;
            }
            if (top.lockman.getLockType(transaction, top.name) == LockType.X) {return LockType.X;}
            if (top.lockman.getLockType(transaction, top.name) == LockType.S) {return LockType.S;}
            top = top.parentContext();
        }
        return eff_lock;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        for (Lock l : lockman.getLocks(transaction)) {
            if ((l.lockType == LockType.SIX) && name.isDescendantOf(l.name)) {return true;}
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> sis_list = new ArrayList<>();
        for (Lock l : lockman.getLocks(transaction)) {
            if (((l.lockType == LockType.S) || (l.lockType == LockType.IS)) && l.name.isDescendantOf(name)) {
                sis_list.add(l.name);
            }
        }
        return sis_list;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {return LockType.NL;}
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }

    //David from office hours helped me to figure out how to use isTableContext() and setTableContext()
    public synchronized boolean isTableContext() {return this.isTableContext;}

    public synchronized void setTableContext(Boolean set) {this.isTableContext = set;}
}

