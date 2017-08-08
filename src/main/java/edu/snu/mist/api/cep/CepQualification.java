package edu.snu.mist.api.cep;

import edu.snu.mist.common.functions.MISTFunction;

import java.util.Map;

/**
 * Cep Qualification to define MISTCepQuery.
 */
public interface CepQualification extends MISTFunction<Map<String, Map<String, Object>>, Boolean> {
}
