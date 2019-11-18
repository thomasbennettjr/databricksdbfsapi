package com.talend.databricks.dbfs;

public class ReadResponse {
    private int bytes_read;
    private String data;

    public int getBytes_read() {
        return bytes_read;
    }

    public void setBytes_read(int bytes_read) {
        this.bytes_read = bytes_read;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
