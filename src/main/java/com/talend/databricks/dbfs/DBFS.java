package com.talend.databricks.dbfs;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Base64;

public class DBFS {
    private String host = null;
    private String token = null;
    private RestTemplate rest = null;
    private static DBFS instance = null;
    private Base64.Encoder base64 = null;
    private ObjectMapper mapper;

    private DBFS(String region, String databricks_token) {
        this.host = "https://"+region+".azuredatabricks.net";
        this.token = databricks_token;

        // Set ObjectMapper
        this.mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


        rest = new RestTemplate();
        rest.setErrorHandler(new CustomResponseErrorHandler());

        this.base64 = Base64.getEncoder();
    }

    public static DBFS getInstance(String region, String databricks_token)
    {
        if (instance == null)
            instance = new DBFS(region, databricks_token);

        return instance;
    }

    public void addBlock(int handler, byte[] data) throws DBFSException
    {
        try {
            HttpEntity<String> requestEntity = null;
            ResponseEntity<String> responseEntity = null;

            String body = "{\"data\": \"" + new String(base64.encode(data), "UTF-8") + "\",\"handle\": "+ handler + "}";
            requestEntity = new HttpEntity<>(body, this.buildHttpHeaders("application/json", "application/json"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/add-block" , HttpMethod.POST, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());
        } catch (UnsupportedEncodingException e)
        {
            throw new DBFSException(e.getMessage());
        }
    }

    public void close(int handler) throws DBFSException
    {
        try {
            HttpEntity<String> requestEntity = null;
            ResponseEntity<String> responseEntity = null;

            String body = "{\"handle\": "+ handler + "}";
            requestEntity = new HttpEntity<>(body, this.buildHttpHeaders("application/json", "application/json"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/close" , HttpMethod.POST, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());
        } catch (Exception e)
        {
            throw new DBFSException(e.getMessage());
        }
    }

    public int create(String path) throws DBFSException
    {
        return this.create(path, false);
    }

    public int create(String path, boolean overwrite) throws DBFSException
    {
        Response response = null;
        try {
            HttpEntity<String> requestEntity = null;
            ResponseEntity<String> responseEntity = null;
            String body = "{\"path\": \""+ path + "\", \"overwrite\": " + overwrite + "}";
            requestEntity = new HttpEntity<>(body, this.buildHttpHeaders("application/json", "application/json"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/create" , HttpMethod.POST, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());
            else
                response = mapper.readValue(responseEntity.getBody(), Response.class);
        } catch (Exception e)
        {
            throw new DBFSException(e.getMessage());
        }

        return response.getHandle();
    }

    public void delete(String path) throws DBFSException
    {
        this.delete(path, false);
    }

    public void delete(String path, boolean recursive) throws DBFSException
    {
        try {
            HttpEntity<String> requestEntity = null;
            ResponseEntity<String> responseEntity = null;
            String body = "{\"path\": \""+ path + "\", \"recursive\": " + recursive + "}";
            requestEntity = new HttpEntity<>(body, this.buildHttpHeaders("application/json", "application/json"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/delete" , HttpMethod.POST, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());

        } catch (Exception e)
        {
            throw new DBFSException(e.getMessage());
        }

    }

    public GetStatusResponse status(String path) throws DBFSException
    {
        GetStatusResponse response = null;
        try {
            HttpEntity<String> requestEntity = null;
            ResponseEntity<String> responseEntity = null;
            requestEntity = new HttpEntity<>(null, this.buildHttpHeaders("application/json", "application/json"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/get-status?path="+path , HttpMethod.GET, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());
            else
                response = mapper.readValue(responseEntity.getBody(), GetStatusResponse.class);
        } catch (Exception e)
        {
            throw new DBFSException(e.getMessage());
        }
        return response;
    }

    public ListResponse list(String path) throws DBFSException
    {
        ListResponse response = null;
        try {
            HttpEntity<String> requestEntity = null;
            ResponseEntity<String> responseEntity = null;
            String body = "{\"path\": "+ path + "}";
            requestEntity = new HttpEntity<>(null, this.buildHttpHeaders("application/json", "application/json"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/list?path="+path , HttpMethod.GET, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());
            else
                response = mapper.readValue(responseEntity.getBody(), ListResponse.class);
        } catch (Exception e)
        {
            throw new DBFSException(e.getMessage());
        }
        return response;
    }

    public void mkdirs(String path) throws DBFSException
    {
        try {
            HttpEntity<String> requestEntity = null;
            ResponseEntity<String> responseEntity = null;
            String body = "{\"path\": \""+ path + "\"}";
            requestEntity = new HttpEntity<>(body, this.buildHttpHeaders("application/json", "application/json"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/mkdirs" , HttpMethod.POST, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());

        } catch (Exception e)
        {
            throw new DBFSException(e.getMessage());
        }

    }

    public void move(String source_path, String destination_path) throws DBFSException
    {
        try {
            HttpEntity<String> requestEntity = null;
            ResponseEntity<String> responseEntity = null;
            String body = "{\"source_path\": \""+ source_path + "\", \"destination_path\":"+destination_path+"\"}";
            requestEntity = new HttpEntity<>(body, this.buildHttpHeaders("application/json", "application/json"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/move" , HttpMethod.POST, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());

        } catch (Exception e)
        {
            throw new DBFSException(e.getMessage());
        }

    }

    public void put(String local_path, String destination_path) throws DBFSException
    {
        try {

            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
            //body.add("file", this.s3Object());
            body.add("file", new FileSystemResource(new File(local_path)));
            body.add("path", destination_path);

            HttpEntity<MultiValueMap<String, Object>> requestEntity = null;
            ResponseEntity<String> responseEntity = null;
            requestEntity = new HttpEntity<>(body, this.buildHttpHeaders("application/json","multipart/form-data"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/put" , HttpMethod.POST, requestEntity, String.class);

            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());

        } catch (Exception e)
        {
            throw new DBFSException(e.getMessage());
        }

    }

    public ReadResponse read(String path, int offset, int length) throws DBFSException
    {
        ReadResponse response = null;
        try {
            HttpEntity<String> requestEntity = null;
            ResponseEntity<String> responseEntity = null;
            String body = "{\"path\": "+ path + "}";
            requestEntity = new HttpEntity<>(null, this.buildHttpHeaders("application/json", "application/json"));

            responseEntity = rest.exchange(host+"/api/2.0/dbfs/read?path="+path+"&offset="+offset+"&length="+length , HttpMethod.GET, requestEntity, String.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful())
                throw new DBFSException(responseEntity.getBody());
            else
                response = mapper.readValue(responseEntity.getBody(), ReadResponse.class);
        } catch (Exception e)
        {
            throw new DBFSException(e.getMessage());
        }
        return response;
    }

    private HttpHeaders buildHttpHeaders(String accept, String contentType)
    {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Accept", accept);
        headers.add("Content-Type", contentType);
        headers.add("Authentication", "Bearer " + this.token);

        return headers;
    }
}
