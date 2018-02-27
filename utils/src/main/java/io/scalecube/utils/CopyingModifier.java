package io.scalecube.utils;

import com.google.common.base.Throwables;

import java.lang.reflect.Constructor;
import java.util.function.Consumer;

public interface CopyingModifier<E> {

  /**
   * Copy and set facility function. Finds on E object a copying constructor, then creates a copy of E and applies given
   * modifier function on copy. Then returns that copy to client.
   */
  default E copyAndSet(Consumer<E> modifier) {
    try {
      // noinspection unchecked
      E cloneable = (E) this;
      Class<?> cloneableClass = cloneable.getClass();
      Constructor<?> copyingConstructor = cloneableClass.getDeclaredConstructor(cloneableClass);
      copyingConstructor.setAccessible(true);
      // noinspection unchecked
      E newInstance = (E) copyingConstructor.newInstance(cloneable);
      modifier.accept(newInstance);
      return newInstance;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
