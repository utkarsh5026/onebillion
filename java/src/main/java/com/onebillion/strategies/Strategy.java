package com.onebillion.strategies;

import java.util.List;

public  interface Strategy {
    List<StationResult> Analyze(String filepath);
}