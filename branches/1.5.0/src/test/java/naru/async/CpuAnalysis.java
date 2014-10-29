package naru.async;

/**
 * Main.java
 * CPU Usage Analysis Tool (CUAT) for -Xhprof ASCII Output.
 */


import java.io.*;
import java.util.*;

/**
 * Entry point class.
 * @version 1.0 2006/03/30
 * @author froo
 */
public class CpuAnalysis {
    private static final String CPU_BLOCK_BOUND =
        "^CPU SAMPLES ";
    private final HashMap traces = new HashMap();
    private final HashMap cpuSamples = new HashMap();

    /**
     * Entry point method.
     * @param args [0] -Xhprof ASCII Output.
     * @throws IOException File input error.
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: java cuat.Main java.hprof.txt");
            System.exit(1);
        }

        CpuAnalysis main = new CpuAnalysis(args[0]);
        main.printTotalPerMethod();
    }

    /**
     * Constructor.
     * @param hprofFile -Xhprof ASCII Output.
     * @throws IOException File input error.
     */
    public CpuAnalysis(String hprofFile) throws IOException {
        parseHprofFile(hprofFile);
    }

    /**
     * 
     */
    public void printTotalPerMethod() {
        HashMap methodMap = totalPerMethod();

        Iterator it = methodMap.keySet().iterator();
        while (it.hasNext()) {
            String method = (String) it.next();
            Integer count = (Integer) methodMap.get(method);
            System.out.println(count + "\t" + method);
        }
    }

    /**
     * @param fileName -Xhprof ASCII Output.
     * @throws IOException File input error.
     */
    private void parseHprofFile(String fileName) throws IOException {
        BufferedReader is = new BufferedReader(new FileReader(fileName));

        try {
            parseTraceBlock(is);
            parseCPUBlock(is);
        } finally {
            is.close();
        }
    }

    private void parseTraceBlock(BufferedReader is) throws IOException {
        String line = null;
        String traceNo = "";
        TreeSet stackSet = new TreeSet();

        while ((line = is.readLine()) != null) {
            if (line.matches(CPU_BLOCK_BOUND + "BEGIN.*")) {
                break;
            }

            if (line.matches("^TRACE [0-9]+:.*")) {
                if (!traceNo.equals("")) {
                    traces.put(traceNo, stackSet);
                    traceNo = "";
                    stackSet = new TreeSet();
                }
                traceNo = line.replaceFirst("^TRACE ", "");
                traceNo = traceNo.replaceFirst("(?<=[0-9]+).*", "");
            }

            if (!traceNo.equals("") && line.matches("^\\t.*")) {
                String method = line.replaceFirst("^\\t", "");
                method = method.replaceFirst("(?<=.+?)\\(.*", "");
                stackSet.add(method);
            }
        }
        if (!traceNo.equals("")) {
            traces.put(traceNo, stackSet);
        }
    }

    private void parseCPUBlock(BufferedReader is) throws IOException {
        String line = null;

        while ((line = is.readLine()) != null) {
            if (line.matches(CPU_BLOCK_BOUND + "END.*")) {
                break;
            }

            if (line.matches("^\\s*[0-9]+\\s+.*")) {
                String tmp = line.replaceFirst("^\\s*([^\\s]+\\s+){3}", "");
                String count = tmp.replaceFirst("(?<=[0-9]+).*", "");
                tmp = tmp.replaceFirst("^([^\\s]+\\s+){1}", "");
                String trace = tmp.replaceFirst("(?<=[0-9]+).*", "");
                cpuSamples.put(trace, count);
            }
        }
    }

    /**
     * @return Method-Count map.
     */
    private HashMap totalPerMethod() {
        HashMap methodMap = new HashMap();

        Iterator it = cpuSamples.keySet().iterator();
        while (it.hasNext()) {
            String traceNo = (String) it.next();
            int count = Integer.parseInt((String) cpuSamples.get(traceNo));
            TreeSet stackSet = (TreeSet) traces.get(traceNo);
            if (stackSet == null) {
                System.err.println("warning: trace[" + traceNo + "]");
                continue;
            }

            Iterator stackIt = stackSet.iterator();
            while (stackIt.hasNext()) {
                String method = (String) stackIt.next();

                Integer totalObj = (Integer) methodMap.get(method);
                int total = 0;
                if (totalObj == null) {
                    total = count;
                } else {
                    total = totalObj.intValue() + count;
                }

                methodMap.put(method, new Integer(total));
            }
        }

        return methodMap;
    }
}