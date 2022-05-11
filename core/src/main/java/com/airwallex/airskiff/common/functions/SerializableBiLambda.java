package com.airwallex.airskiff.common.functions;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBiLambda<T, U, R> extends BiFunction<T, U, R>, Serializable {
}
