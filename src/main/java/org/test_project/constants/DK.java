package org.test_project.constants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class DK {

    public abstract static class Name {
        public final static String _01 = "HTTP DOWNLINK - NUMBER OF ATTEMPTS [N]";
        public final static String _02 = "HTTP DOWNLINK - NUMBER OF DNS HOST RESOLUTION FAILURES [N]";
        public final static String _03 = "HTTP DOWNLINK - NUMBER OF IP SERVICE ACCESS FAILURES [N]";
        public final static String _04 = "HTTP DOWNLINK - NUMBER OF DNS+IP SERVICE ACCESS TIMEOUT  [N]";
        public final static String _05 = "HTTP DOWNLINK - NUMBER OF ERRORS IN RETAINABILITY [N]";
        public final static String _06 = "HTTP DOWNLINK - DOWNLOAD SPEED [KBIT/S] AVG";
        public final static String _07 = "HTTP DOWNLINK - THROUGHPUT STANDARD DEVIATION";
        public final static String _08 = "HTTP DOWNLINK - DOWNLOAD Session Time (DNS TIME + IP SERVICE ACCESS TIME + TRANSFER TIME) [S]";
        public final static String _10 = "HTTP DOWNLINK - D2. PERCENTAGE OF DL CONNECTIONS WITH THROUGHPUT > 3 MBPS";
        public final static String _11 = "HTTP DOWNLINK - NUMBER OF DL WITH THROUGHPUT > 3 MBPS";
        public final static String _16 = "HTTP DOWNLINK - PEAK DATA USER RATE [KBIT/S]";
        public final static String _17 = "HTTP DOWNLINK - 10TH PERCENTILE THR.";
        public final static String _18 = "HTTP DOWNLINK - 90TH PERCENTILE THR.";
        public final static String _19 = "HTTP DOWNLINK - SUCCESS RATE [%]";
        public final static String _20 = "HTTP DOWNLINK - FAILURE RATE [%]";
        public final static String _21 = "HTTP DOWNLINK - DNS HOST RESOLUTION FAILURE RATE [%]";
        public final static String _22 = "HTTP DOWNLINK - IP SERVICE ACCESS FAILURE RATE [%]";
        public final static String _23 = "HTTP DOWNLINK - DNS+IP SERVICE ACCESS TIMEOUT RATE  [%]";
        public final static String _24 = "HTTP DOWNLINK - RETAINABILITY FAILURE RATE [%]";


    }

    public abstract static class Kpi_ids {

        public final static List<String> _01 = Arrays.asList("75500", "675500", "575500");
        public final static List<String> _02 = Arrays.asList("75500", "675500", "575500");
        public final static List<String> _03 = Arrays.asList("75501", "675501", "575501");
        public final static List<String> _04 = Arrays.asList("75504", "675504", "575504");
        public final static List<String> _05 = Arrays.asList("75502", "675502", "575502");
        public final static List<String> _06 = Arrays.asList("75503", "675503", "575503");
        public final static List<String> _07 = Arrays.asList("75503", "675503", "575503");
        public final static Map<String, List<String>> _08 = new HashMap<String, List<String>>() {{
            put("4G", Arrays.asList("75500", "75501", "75502"));
            put("5GCap", Arrays.asList("675500", "675501", "675502"));
            put("5GReal", Arrays.asList("575500", "575501", "575502"));
            put("4G_5GCap", Arrays.asList("75500", "75501", "75502", "675500", "675501", "675502"));
        }};
        public final static List<String> _10 = Arrays.asList("75503", "675503", "575503");
        public final static List<String> _11 = Arrays.asList("75503", "675503", "575503");
        public final static List<String> _16 = Arrays.asList("75503", "675503", "575503");
        public final static List<String> _17 = Arrays.asList("75503", "675503", "575503");
        public final static List<String> _18 = Arrays.asList("75503", "675503", "575503");
        public final static Map<String, List<String>> _19 = new HashMap<String, List<String>>() {{
            put("4G", Arrays.asList("75500", "75500", "75501", "75504", "75502"));
            put("5GCap", Arrays.asList("675500", "675500", "675501", "675504", "675502"));
            put("5GReal", Arrays.asList("575500", "575500", "575501", "575504", "575502"));
        }};
        public final static Map<String, List<String>> _20 = new HashMap<String, List<String>>() {{
            put("4G", Arrays.asList("75500", "75500", "75501", "75504", "75502"));
            put("5GCap", Arrays.asList("675500", "675500", "675501", "675504", "675502"));
            put("5GReal", Arrays.asList("575500", "575500", "575501", "575504", "575502"));
        }};
        public final static Map<String, List<String>> _21 = new HashMap<String, List<String>>() {{
            put("4G", Arrays.asList("75500", "75500"));
            put("5GCap", Arrays.asList("675500", "675500"));
            put("5GReal", Arrays.asList("575500", "575500"));
        }};
        public final static Map<String, List<String>> _22 = new HashMap<String, List<String>>() {{
            put("4G", Arrays.asList("75500", "75501"));
            put("5GCap", Arrays.asList("675500", "675501"));
            put("5GReal", Arrays.asList("575500", "575501"));
        }};
        public final static Map<String, List<String>> _23 = new HashMap<String, List<String>>() {{
            put("4G", Arrays.asList("75500", "75504"));
            put("5GCap", Arrays.asList("675500", "675504"));
            put("5GReal", Arrays.asList("575500", "575504"));
        }};
        public final static List<String> _24 = Arrays.asList("75502", "675502", "575502");
    }


}
