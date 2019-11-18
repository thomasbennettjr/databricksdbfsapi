package com.talend.databricks.dbfs;

public class ListResponse {
    private GetStatusResponse[] files;

    public GetStatusResponse[] getFiles() {
        return files;
    }

    public void setFiles(GetStatusResponse[] files) {
        this.files = files;
    }
}
