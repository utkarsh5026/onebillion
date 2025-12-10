package com.onebillion.strategies;

import java.io.IOException;
import java.util.List;

public interface Strategy {
  List<StationResult> Analyze(String filepath) throws IOException;
}
