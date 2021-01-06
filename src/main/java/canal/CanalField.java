package canal;

import java.util.List;

/**
 * @Author: LailaiMonkey
 * @Description：
 * @Date：Created in 2021-01-05 14:03
 * @Modified By：
 */
public class CanalField {

    private String id;

    private List<String> fields;

    private List<String> values;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
}
