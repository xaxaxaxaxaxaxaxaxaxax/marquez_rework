/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.logging;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.MDC;

/**
 * MDC uses ThreadLocal to store the context, and this is not propagated when the logic runs in
 * separate thread. This class provides static wrapper methods to propagate the MDC context to the
 * thread where the logic runs.
 */
public class MdcPropagating {
  public static <T> Supplier<T> withMdc(Supplier<T> supplier) {
    Map<String, String> mdcContext = MDC.getCopyOfContextMap();
    return () -> {
      Map<String, String> previousContext = MDC.getCopyOfContextMap();
      setContext(mdcContext);
      try {
        return supplier.get();
      } finally {
        setContext(previousContext);
      }
    };
  }

  public static <T> Consumer<T> withMdc(Consumer<T> consumer) {
    Map<String, String> mdcContext = MDC.getCopyOfContextMap();
    return (t) -> {
      Map<String, String> previousContext = MDC.getCopyOfContextMap();
      setContext(mdcContext);
      try {
        consumer.accept(t);
      } finally {
        setContext(previousContext);
      }
    };
  }

  public static Runnable withMdc(Runnable runnable) {
    Map<String, String> mdcContext = MDC.getCopyOfContextMap();
    return () -> {
      Map<String, String> previousContext = MDC.getCopyOfContextMap();
      setContext(mdcContext);
      try {
        runnable.run();
      } finally {
        setContext(previousContext);
      }
    };
  }

  private static void setContext(Map<String, String> context) {
    if (context == null) {
      MDC.clear();
    } else {
      MDC.setContextMap(context);
    }
  }
}
