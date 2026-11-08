// SpdxTemplateDiff: compares license texts with SPDX license templates (SPDX matching guidelines),
// with the SPDX Java library of spdx tools-java. One JVM for many comparisons.
//
// Input on stdin, one request per line: <file path> TAB <SPDX id>[,<SPDX id>...]
// Output on stdout, one JSON object per request and id:
//   {"path": ..., "id": ..., "match": true|false, "within": true|false, "message": ..., "error": ...}
// "match": the whole text matches the template. "within": the template text is found inside the text
// (a notice or other lines around the license text).
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.spdx.library.ListedLicenses;
import org.spdx.library.model.v2.license.SpdxListedLicense;
import org.spdx.utility.compare.CompareTemplateOutputHandler.DifferenceDescription;
import org.spdx.utility.compare.LicenseCompareHelper;

public class SpdxTemplateDiff {
    static String q(String s) {
        if (s == null) return "null";
        StringBuilder b = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default:
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c)); else b.append(c);
            }
        }
        return b.append('"').toString();
    }

    public static void main(String[] args) throws Exception {
        ListedLicenses ll = ListedLicenses.getListedLicenses();
        if (args.length > 0 && args[0].equals("--version")) {
            System.out.println("{\"license_list_version\": " + q(ll.getLicenseListVersion()) + "}");
            return;
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            if (line.isBlank()) continue;
            String[] parts = line.split("\t", 2);
            String path = parts[0];
            String text;
            try {
                text = new String(Files.readAllBytes(Path.of(path)), StandardCharsets.UTF_8);
            } catch (Exception e) {
                System.out.println("{\"path\": " + q(path) + ", \"error\": " + q("read: " + e) + "}");
                continue;
            }
            if (parts.length < 2) continue;
            for (String id : parts[1].split(",")) {
                id = id.trim();
                if (id.isEmpty()) continue;
                String out;
                try {
                    if (!ll.isSpdxListedLicenseId(id)) {
                        out = "{\"path\": " + q(path) + ", \"id\": " + q(id) + ", \"error\": \"not a listed license id\"}";
                    } else {
                        SpdxListedLicense lic = ll.getListedLicenseByIdCompatV2(id);
                        DifferenceDescription d = LicenseCompareHelper.isTextStandardLicense(lic, text);
                        boolean match = !d.isDifferenceFound();
                        boolean within = match || LicenseCompareHelper.isStandardLicenseWithinText(text, lic);
                        out = "{\"path\": " + q(path) + ", \"id\": " + q(id) + ", \"match\": " + match
                            + ", \"within\": " + within + ", \"message\": " + q(match ? "" : d.getDifferenceMessage()) + "}";
                    }
                } catch (Exception e) {
                    out = "{\"path\": " + q(path) + ", \"id\": " + q(id) + ", \"error\": " + q(e.toString()) + "}";
                }
                System.out.println(out);
                System.out.flush();
            }
        }
    }
}
