package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import java.util.ArrayList;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction==null) {return;}
        if (lockType == LockType.NL) {return;}
        if (lockContext.readonly) {return;}
        LockContext context = lockContext;
        List<LockContext> ancestors_list = new ArrayList<>();
        while (context.parent!=null) {
            context = context.parent;
            ancestors_list.add(context);
        }
        for (int i=ancestors_list.size(); i>0; i--) {
            LockContext ancestor = ancestors_list.get(i-1);
            LockType ancestor_type = ancestor.getEffectiveLockType(transaction);
            if (ancestor_type == LockType.IS && lockType == LockType.X) {ancestor.promote(transaction, LockType.IX);}
            if (ancestor_type == LockType.S && lockType == LockType.X) {ancestor.promote(transaction, LockType.SIX);}
            if (ancestor_type == LockType.NL) {
                if (lockType == LockType.S) {ancestor.acquire(transaction, LockType.IS);}
                if (lockType == LockType.X) {ancestor.acquire(transaction, LockType.IX);}
            }
        }
        LockType type = lockContext.getExplicitLockType(transaction);
        if (type == LockType.IX && lockType == LockType.S) {
            lockContext.promote(transaction, LockType.SIX);
            return;
        }
        if (type != LockType.NL) {
            if (lockContext.parentContext().isTableContext()) {
                if (lockContext.parentContext().capacity >= 10) {
                    if (lockContext.parentContext().saturation(transaction) >= 0.2) {
                        lockContext.parentContext().escalate(transaction);
                    }
                }
            }
        }
        type = lockContext.getEffectiveLockType(transaction);
        if (LockType.substitutable(type, lockType)) {return;}
        type = lockContext.getExplicitLockType(transaction);
        if (type == LockType.NL) {
            lockContext.acquire(transaction, lockType);
        } else {
            if (!LockType.substitutable(type, lockType)) {lockContext.promote(transaction, lockType);}
        }
    }

    public static void ensureAncestors(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement
        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction==null) {return;}
        if (lockType == LockType.NL) {return;}
        if (lockContext.readonly) {return;}
        LockContext context = lockContext;
        List<LockContext> ancestors_list = new ArrayList<>();
        while (context.parent!=null) {
            context = context.parent;
            ancestors_list.add(context);
        }
        for (int i=ancestors_list.size(); i>0; i--) {
            LockContext ancestor = ancestors_list.get(i-1);
            LockType ancestor_type = ancestor.getEffectiveLockType(transaction);
            if (ancestor_type == LockType.IS && lockType == LockType.X) {ancestor.promote(transaction, LockType.IX);}
            if (ancestor_type == LockType.S && lockType == LockType.X) {ancestor.promote(transaction, LockType.SIX);}
            if (ancestor_type == LockType.NL) {
                if (lockType == LockType.S) {ancestor.acquire(transaction, LockType.IS);}
                if (lockType == LockType.X) {ancestor.acquire(transaction, LockType.IX);}
            }
        }
    }

}
