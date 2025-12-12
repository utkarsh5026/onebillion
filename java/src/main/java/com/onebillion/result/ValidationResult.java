package com.onebillion.result;

import java.util.ArrayList;
import java.util.List;

public class ValidationResult {
  boolean isValid;
  int totalStations;
  int matchedStations;
  int mismatchedStations;
  int missingStations;
  int extraStations;
  List<String> errors;

  public ValidationResult() {
    this.errors = new ArrayList<>();
  }

  public ValidationResult addError(String error) {
    isValid = false;
    this.errors.add(error);
    return this;
  }
}
