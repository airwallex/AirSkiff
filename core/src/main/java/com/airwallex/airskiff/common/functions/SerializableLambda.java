package com.airwallex.airskiff.common.functions;

import java.io.Serializable;
import java.util.function.Function;

public interface SerializableLambda<I, O> extends Function<I, O>, Serializable {}
