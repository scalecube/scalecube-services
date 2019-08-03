package io.scalecube.services;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Objects;

/** Utility to work with Java 5 generic type parameters. */
public class TypeUtils {

  private TypeUtils() {
    // Do not instantiate
  }

  /**
   * Check if the right-hand side type may be assigned to the left-hand side type following the Java
   * generics rules.
   *
   * @param lhsType the target type
   * @param rhsType the value type that should be assigned to the target type
   * @return true if rhs is assignable to lhs
   */
  public static boolean isAssignable(Type lhsType, Type rhsType) {
    Objects.requireNonNull(lhsType, "Left-hand side type must not be null");
    Objects.requireNonNull(rhsType, "Right-hand side type must not be null");

    // all types are assignable to themselves and to class Object
    if (lhsType.equals(rhsType) || Object.class == lhsType) {
      return true;
    }

    if (lhsType instanceof Class) {
      Class<?> lhsClass = (Class<?>) lhsType;

      // just comparing two classes
      if (rhsType instanceof Class) {
        return ClassUtils.isAssignable(lhsClass, (Class<?>) rhsType);
      }

      if (rhsType instanceof ParameterizedType) {
        Type rhsRaw = ((ParameterizedType) rhsType).getRawType();

        // a parameterized type is always assignable to its raw class type
        if (rhsRaw instanceof Class) {
          return ClassUtils.isAssignable(lhsClass, (Class<?>) rhsRaw);
        }
      } else if (lhsClass.isArray() && rhsType instanceof GenericArrayType) {
        Type rhsComponent = ((GenericArrayType) rhsType).getGenericComponentType();

        return isAssignable(lhsClass.getComponentType(), rhsComponent);
      }
    }

    // parameterized types are only assignable to other parameterized types and class types
    if (lhsType instanceof ParameterizedType) {
      if (rhsType instanceof Class) {
        Type lhsRaw = ((ParameterizedType) lhsType).getRawType();

        if (lhsRaw instanceof Class) {
          return ClassUtils.isAssignable((Class<?>) lhsRaw, (Class<?>) rhsType);
        }
      } else if (rhsType instanceof ParameterizedType) {
        return isAssignable((ParameterizedType) lhsType, (ParameterizedType) rhsType);
      }
    }

    if (lhsType instanceof GenericArrayType) {
      Type lhsComponent = ((GenericArrayType) lhsType).getGenericComponentType();

      if (rhsType instanceof Class) {
        Class<?> rhsClass = (Class<?>) rhsType;

        if (rhsClass.isArray()) {
          return isAssignable(lhsComponent, rhsClass.getComponentType());
        }
      } else if (rhsType instanceof GenericArrayType) {
        Type rhsComponent = ((GenericArrayType) rhsType).getGenericComponentType();

        return isAssignable(lhsComponent, rhsComponent);
      }
    }

    if (lhsType instanceof WildcardType) {
      return isAssignable((WildcardType) lhsType, rhsType);
    }

    return false;
  }

  private static boolean isAssignable(ParameterizedType lhsType, ParameterizedType rhsType) {
    if (lhsType.equals(rhsType)) {
      return true;
    }

    Type[] lhsTypeArguments = lhsType.getActualTypeArguments();
    Type[] rhsTypeArguments = rhsType.getActualTypeArguments();

    if (lhsTypeArguments.length != rhsTypeArguments.length) {
      return false;
    }

    for (int size = lhsTypeArguments.length, i = 0; i < size; ++i) {
      Type lhsArg = lhsTypeArguments[i];
      Type rhsArg = rhsTypeArguments[i];

      if (!lhsArg.equals(rhsArg)
          && !(lhsArg instanceof WildcardType && isAssignable((WildcardType) lhsArg, rhsArg))) {
        return false;
      }
    }

    return true;
  }

  private static boolean isAssignable(WildcardType lhsType, Type rhsType) {
    Type[] lhsUpperBounds = lhsType.getUpperBounds();

    // supply the implicit upper bound if none are specified
    if (lhsUpperBounds.length == 0) {
      lhsUpperBounds = new Type[] {Object.class};
    }

    Type[] lhsLowerBounds = lhsType.getLowerBounds();

    // supply the implicit lower bound if none are specified
    if (lhsLowerBounds.length == 0) {
      lhsLowerBounds = new Type[] {null};
    }

    if (rhsType instanceof WildcardType) {
      // both the upper and lower bounds of the right-hand side must be completely enclosed in the
      // upper and lower bounds of the left-hand side.
      WildcardType rhsWcType = (WildcardType) rhsType;
      Type[] rhsUpperBounds = rhsWcType.getUpperBounds();

      if (rhsUpperBounds.length == 0) {
        rhsUpperBounds = new Type[] {Object.class};
      }

      Type[] rhsLowerBounds = rhsWcType.getLowerBounds();

      if (rhsLowerBounds.length == 0) {
        rhsLowerBounds = new Type[] {null};
      }

      for (Type lhsBound : lhsUpperBounds) {
        for (Type rhsBound : rhsUpperBounds) {
          if (!isAssignableBound(lhsBound, rhsBound)) {
            return false;
          }
        }

        for (Type rhsBound : rhsLowerBounds) {
          if (!isAssignableBound(lhsBound, rhsBound)) {
            return false;
          }
        }
      }

      for (Type lhsBound : lhsLowerBounds) {
        for (Type rhsBound : rhsUpperBounds) {
          if (!isAssignableBound(rhsBound, lhsBound)) {
            return false;
          }
        }

        for (Type rhsBound : rhsLowerBounds) {
          if (!isAssignableBound(rhsBound, lhsBound)) {
            return false;
          }
        }
      }
    } else {
      for (Type lhsBound : lhsUpperBounds) {
        if (!isAssignableBound(lhsBound, rhsType)) {
          return false;
        }
      }

      for (Type lhsBound : lhsLowerBounds) {
        if (!isAssignableBound(rhsType, lhsBound)) {
          return false;
        }
      }
    }

    return true;
  }

  private static boolean isAssignableBound(Type lhsType, Type rhsType) {
    if (rhsType == null) {
      return true;
    }
    if (lhsType == null) {
      return false;
    }
    return isAssignable(lhsType, rhsType);
  }
}
