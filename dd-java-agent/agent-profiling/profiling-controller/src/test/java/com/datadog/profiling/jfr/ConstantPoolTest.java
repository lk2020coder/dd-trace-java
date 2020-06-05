package com.datadog.profiling.jfr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class ConstantPoolTest {
  private ConstantPool instance;

  @BeforeEach
  void setUp() {
    Type type = Mockito.mock(Type.class);
    Mockito.when(type.canAccept(ArgumentMatchers.any())).thenReturn(true);

    TypedValue nullValue = new TypedValue(type, null, 0);

    Mockito.when(type.nullValue()).thenReturn(nullValue);

    instance = new ConstantPool(type);
  }

  @Test
  void addOrGetNull() {
    TypedValue value = instance.addOrGet(null);
    assertNotNull(value);
    assertTrue(value.isNull());
  }

  @Test
  void addOrGetNonNull() {
    Object objectValue = "hello";
    TypedValue value = instance.addOrGet(objectValue);
    assertNotNull(value);
    assertFalse(value.isNull());
    assertEquals(objectValue, value.getValue());
  }

  @Test
  void getNegativeIndex() {
    assertNull(instance.get(-1));
  }

  @Test
  void getNonExistent() {
    assertNull(instance.get(100));
  }

  @Test
  void get() {
    Object objectValue = "hello";
    TypedValue value = instance.addOrGet(objectValue);

    assertEquals(value, instance.get(value.getCPIndex()));
  }
}