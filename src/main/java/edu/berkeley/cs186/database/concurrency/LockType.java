package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // Done

        // NL is compatible with all lock types
        if (a.equals(NL) || b.equals(NL)) {return true;}

        // IS and IS
        if (a.equals(IS) && b.equals(IS)) {return true;}

        // IS and IX
        if ((a.equals(IS) && b.equals(IX)) ||(a.equals(IX) && b.equals(IS))) {return true;}

        // IS and S
        if ((a.equals(IS) && b.equals(S)) ||(a.equals(S) && b.equals(IS))) {return true;}

        // IS and SIX
        if ((a.equals(IS) && b.equals(SIX)) ||(a.equals(SIX) && b.equals(IS))) {return true;}

        // IX and IX
        if (a.equals(IX) && b.equals(IX)) {return true;}

        // S and S
        if (a.equals(S) && b.equals(S)) {return true;}
        // Case 1: If either lock is NL then this is compatible with all other lock types
        if (a == NL || b == NL) {return true;}

        // Case 2: One of the locks is IS
        // The only combination that is not allowed is (X, IS)
        if ((a == IS || b == IS) && (a == X || b == X)) {return false;}

        // Case 3: One of the locks is IX
        // Illegal combo 1: (IX, S)
        if ((a == IX || b == IX) && (a == S || b == S)) {return false;}
        // Illegal combo 2: (IX, SIX)
        if ((a == IX || b == IX) && (a == SIX || b == SIX)) {return false;}
        // Illegal combo 3: (IX, X)
        if ((a == IX || b == IX) && (a == X || b == X)) {return false;}

        // Case 4: one of the locks is S
        // illegal combo 1: (S, IX)
        // redundant; covered above
        if ((a == S || b == S) && (a == IX || b == IX)) {return false;}
        // illegal combo 2: (S, SIX)
        if ((a == S || b == S) && (a == SIX || b == SIX)) {return false;}
        // illegal combo 3: (S, X)
        if ((a == S || b == S) && (a == X || b == X)) {return false;}

        // Case 5: one of the locks is SIX
        // illegal combo 1: (SIX, IX)
        // covered in the IX combo portions
        // illegal combo 2: (SIX, S)
        // covered in the S combo portions
        // illegal combo 3: (SIX, SIX)
        if ((a == SIX || b == SIX) && (a == SIX || b == SIX)) {return false;}
        // illegal combo 4: (SIX, X)
        if ((a == SIX || b == SIX) && (a == X || b == X)) {return false;}

        // Case 6: one of the locks is X
        // illegal combo 1: (X, IS)
        // covered in IS combos
        // illegal combo 2: (X, IX)
        // covered in the IX combo portions
        // illegal combo 3: (X, S)
        // covered in S combos
        // illegal combo 4: (X, SIX)
        // covered in SIX combos
        // illegal combo 5: (X, X)
        if ((a == X || b == X) && (a == X || b == X)) {return false;}

        return true;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // Done

        if (childLockType.equals(NL)) {return true;}

        if (parentLockType.equals(IS) && childLockType.equals(IS)) {return true;}

        if (parentLockType.equals(IS) && childLockType.equals(S)) {return true;}

        if (parentLockType.equals(IX) && childLockType.equals(IX)) {return true;}

        if (parentLockType.equals(IX) && childLockType.equals(SIX)) {return true;}

        if (parentLockType.equals(IX) && childLockType.equals(S)) {return true;}

        if (parentLockType.equals(IX) && childLockType.equals(X)) {return true;}

        if (parentLockType.equals(SIX) && childLockType.equals(IX)) {return true;}

        if (parentLockType.equals(SIX) && childLockType.equals(X)) {return true;}

        if ((parentLockType == IS || parentLockType == IX) && (childLockType == S || childLockType == IS)) {return true;}
        if ((parentLockType == IX || parentLockType == SIX) && (childLockType == X || childLockType == IX)) {return true;}
        if (childLockType == NL) {return true;}

        else {
            return parentLockType == parentLock(childLockType);
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        // Done

        if (required.equals(NL)) {return true;}

        if (required.equals(substitute)) {return true;}

        // Valid
        if (required.equals(IS) && substitute.equals(IX)) {return true;}

        //if (required.equals(IS) && substitute.equals(S)) {return true;}

        // Valid
        if (required.equals(S) && substitute.equals(SIX)) {return true;}

        // Valid
        if (required.equals(IX) && substitute.equals(SIX)) {return true;}

        // Valid
        if (required.equals(IS) && substitute.equals(SIX)) {return true;}

        // Valid
        if (required.equals(S) && substitute.equals(X)) {return true;}

        if (required.equals(IX) && substitute.equals(X)) {return true;}

        //if (required.equals(IS) && substitute.equals(X)) {return true;}

        if (required == NL) {return true;}
        if (required == S && substitute == IS) {return false;}
        if (required == S && substitute == IX) {return false;}
        if (required == X && substitute == S) {return false;}
        if (required == X && substitute == IS) {return false;}
        if (required == X && substitute == IX) {return false;}
        if (required == X && substitute == SIX) {return false;}

        return true;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

