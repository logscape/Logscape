package com.liquidlabs.log.explore;

import com.liquidlabs.common.collection.Collections;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

import static com.liquidlabs.common.collection.Collections.*;

/**
 * Created by neil on 06/04/16.
 */
public class DirListToJson {


    public JSONObject convert(Map<String, List<String>> urlWFiles, String host) {
        JSONObject jsonObject = new JSONObject();
        JSONArray urls = new JSONArray();
        JSONArray dirs = new JSONArray();
        try {

            jsonObject.put("urls", urls);


            for (String url : urlWFiles.keySet()) {
                urls.put(url);
                int index = urls.length()-1;
                jsonObject.put("format", getFormat(urlWFiles.get(url)));
                makeDirList(host, dirs, urlWFiles.get(url), index);
            }
            jsonObject.put("dirs", sortDirsList(dirs));
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject;

    }

    public JSONArray sortDirsList(JSONArray dirs) {
        List<JSONObject> dirss = new ArrayList<>();
        List<JSONObject> files = new ArrayList<>();
        for (int i = 0; i < dirs.length(); i++) {
            try {
                JSONObject obj = dirs.getJSONObject(i);
                if (!obj.getBoolean("isFile")) {
                    obj.put("children", sortDirsList(obj.getJSONArray("children")));
                    dirss.add(obj);
                } else {
                    files.add(obj);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        java.util.Collections.sort(dirss, new Comparator<JSONObject>() {

            @Override
            public int compare(JSONObject o1, JSONObject o2) {
                try {
                    return o1.getString("name").compareTo(o2.getString("name"));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });
        java.util.Collections.sort(files, new Comparator<JSONObject>() {

            @Override
            public int compare(JSONObject o1, JSONObject o2) {
                try {
                    return o1.getString("name").compareTo(o2.getString("name"));
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });
        JSONArray results = new JSONArray();
        for (JSONObject dd : dirss) {
            results.put(dd);
        }

        for (JSONObject ff : files) {
            results.put(ff);
        }
        return results;
    }

    private String getFormat(List<String> strings) {
        if (strings.size() == 0) return "unix";
        return strings.iterator().next().contains(":") ? "windows" : "unix";
    }

    public JSONArray makeDirList(String hostname, JSONArray dirsJson, List<String> files, int urlIndex) {

        try {

            String format = getFormat(files);
            for (String file : files) {
                boolean isTruncatinghost = hostname.length() > 0 && file.contains(hostname);
                if (format.equals("windows")) {
                    String[] parts = file.split("\\\\");
                    // gets C: or '-' when the host is a fwdr etc
                    JSONObject next = getItem(isTruncatinghost ? "-" : parts[0], file, dirsJson, false, urlIndex);

                    for (int index = 1; index < parts.length; index++) {
                        if (isTruncatinghost) {
                            if (parts[index].equals(hostname)) isTruncatinghost = false;
                            continue;
                        }
                        next = getItem(parts[index], file, next.getJSONArray("children"), index == parts.length - 1, urlIndex);

                    }
                } else {
                    String[] parts = file.split("/");
                    JSONObject next = getItem("/", file, dirsJson, false, urlIndex);
                    for (int index = 0; index < parts.length; index++) {
//                        System.out.println("processing:" + parts[index]);
                        if (parts[index].length() == 0) continue;
                        if (isTruncatinghost) {
                            if (parts[index].equals(hostname)) isTruncatinghost = false;
                            continue;
                        }

                        next = getItem(parts[index], file, next.getJSONArray("children"), index == parts.length - 1, urlIndex);
                    }
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return dirsJson;


    }
    private JSONObject getItem(String drive, String path, JSONArray dirs, boolean isFile, int urlIndex) throws JSONException {
        JSONObject arrayItem = null;
        for (int i = 0; i < dirs.length(); i++) {
            if (dirs.getJSONObject(i).get("name").equals(drive)) {
                arrayItem = dirs.getJSONObject(i);
            }

        }
        if (arrayItem == null) {
            JSONObject driveItem = new JSONObject();
            driveItem.put("name", drive);
            driveItem.put("isFile", isFile);
            if (isFile) {
                driveItem.put("path", path);
                driveItem.put("url", urlIndex);
            } else {
                driveItem.put("children", new JSONArray());
            }
            arrayItem = driveItem;
            dirs.put(driveItem);
        }

        return arrayItem;
    }

}
