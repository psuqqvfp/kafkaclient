package kafka.model;

/**
 * User: Administrator
 * Date: 2016/7/19-11:47
 */
public class KeyWord {
    private String id;
    private String user;
    private String keyword;
    private String data;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return id + " " + user + " " + keyword + " " +data;
    }
}
