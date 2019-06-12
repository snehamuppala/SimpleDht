package edu.buffalo.cse.cse486586.simpledht;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import android.content.Context;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    String myPort = null;
    String portStr = null;
    String portHashed = null;
    String key = null;
    String value = null;
    private final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String SERVER = "Server";
    static final String CLIENT = "Client";
    static final String QUERY = "Query";
    String hashedPredecessor = null;
    String predecessor = null;
    String hashedSuccessor = null;
    String successor = null;


    NavigableMap<String, String> avdList = new TreeMap<String, String>(new Comparator<String>() {
        @Override
        public int compare(String key1, String key2) {
            return key1.compareTo(key2);
        }
    });

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        predecessor = myPort;
        successor = myPort;
        portHashed="";
        String messageCreate = "";
        messageCreate = generateMessage(portStr, "Addme", predecessor, successor, key, value);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            portHashed = genHash(portStr);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            return false;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        avdList.put(portHashed, portStr);
        if (!portStr.equals("5554")) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageCreate);
        }
        return false;
    }
    private boolean canInsertToThisAvd(String key) throws Exception{

        boolean isCorrectAvd = false;
        String hashedKey = genHash(key);

        if (myPort.equals(successor) && myPort.equals(predecessor)) {
            isCorrectAvd = true;
        } else if ((hashedPredecessor.compareTo(portHashed)>0 && hashedSuccessor.compareTo(portHashed)>0) && (hashedKey.compareTo(hashedPredecessor) > 0 || hashedKey.compareTo(portHashed) < 0)) {
            isCorrectAvd = true;
        }else if (hashedKey.compareTo(hashedPredecessor) > 0 && hashedKey.compareTo(portHashed) < 0) {
            isCorrectAvd = true;
        }

        return isCorrectAvd;
    }
    /*
    Reference : https://stackoverflow.com/questions/6030744/android-reading-from-file-openfileinput
    */
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        key = values.getAsString("key");
        value = values.getAsString("value");
        try {
            if (canInsertToThisAvd(key)) {
                FileOutputStream outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            } else {
                String messageSend = generateMessage(portStr, "send", "c", "c", key, value);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageSend);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /*

    1.Reference: https://stackoverflow.com/questions/14768191/how-do-i-read-the-file-content-from-the-internal-storage-android-app
    2.Reference: https://stackoverflow.com/questions/9435158/how-to-populate-listview-from-matrix-cursor
    3.https://stackoverflow.com/questions/14317793/android-instrumentationtestcase-getfilesdir-returns-null
    */
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        String fileMessage = " ";
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

        if (selection.equals("@")) {
            String content[] = getContext().fileList();
            for (String file : content) {
                try {
                    FileInputStream inputstream = getContext().openFileInput(file);
                    InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
                    BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
                    fileMessage = bufferedReader.readLine();
                    matrixCursor.addRow(new Object[]{file, fileMessage});
                    inputstream.close();
                }  catch (Exception e) {
                    e.printStackTrace();
                    Log.e("QUERY", e.getLocalizedMessage());
                }
            }
            return matrixCursor;
        } else if (selection.equals("*")) {
            try {
                String content[] = getContext().fileList();
                for (String file : content) {
                    FileInputStream inputstream = getContext().openFileInput(file);
                    InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
                    BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
                    fileMessage = bufferedReader.readLine();
                    matrixCursor.addRow(new Object[]{file, fileMessage});
                    inputstream.close();
                }
                Log.v(QUERY, "Checking whether to forward request to other avds or not");
                Log.v(QUERY, "Size is " + avdList.size());
                if (avdList.size() > 1) {

                    String originalPort = portStr;
                    if (selectionArgs != null && selectionArgs.length != 0) {
                        originalPort = selectionArgs[0];
                        if (successor.equals(originalPort)) {
                            Log.d(QUERY, "Success: Have reached the initial avd which had requested *, hence stopping here");
                            return matrixCursor;
                        }
                    }
                    int port=Integer.parseInt(successor) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port);

                    String messageSend = generateMessage(portStr, "queryAll", " ", originalPort, selection, " ");

                    PrintWriter writer = new PrintWriter(socket.getOutputStream());
                    writer.println(messageSend);
                    writer.flush();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        if (line.length() > 0) {
                            String[] t = line.split(":");
                            String k = t[0];
                            String v = t[1];
                            matrixCursor.addRow(new Object[]{k, v});
                        }
                    }

                    socket.close();
                    reader.close();
                    writer.close();
                }

            } catch(Exception e) {
                Log.e("QUERY", e.getLocalizedMessage());
            }
            return matrixCursor;
        } else {
            try {
                FileInputStream inputstream = getContext().openFileInput(selection);
                InputStreamReader inputstreamreader = new InputStreamReader(inputstream);
                BufferedReader bufferedReader = new BufferedReader(inputstreamreader);
                fileMessage = bufferedReader.readLine();
                Log.v(QUERY, "Value found for query");
                inputstream.close();
            } catch (Exception e) {

                Log.v("Query", portStr+" could not find the key-value, sending selection to successor " + successor);
                String msg_to_forward = generateMessage(portStr, "helpThePoorGuy", "c", "c", selection, " ");

                Socket socket = null;
                try {
                    int port=Integer.parseInt(successor) * 2;
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port);

                    PrintWriter writer = new PrintWriter(socket.getOutputStream());
                    writer.println(msg_to_forward);
                    writer.flush();

                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    fileMessage = reader.readLine();

                    Log.v(QUERY, "Value received at last "+fileMessage);

                    reader.close();
                    writer.close();
                    socket.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

            }
            matrixCursor.addRow(new Object[]{selection, fileMessage});
            return matrixCursor;
        }
    }
    /*
    Reference: https://stackoverflow.com/questions/55531858/how-to-delete-a-file-using-the-path
     */

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String content[] = getContext().fileList();
        try {

            if (selection.equals("*") || selection.equals("@")) {
                for (String file : content) {
                    getContext().deleteFile(file);
                }

            } else {
                getContext().deleteFile(selection);
            }
        } catch (Exception e) {
            Log.e("DELETE", e.getLocalizedMessage());
        }
        return 0;
    }

    // TODO Auto-generated method stub


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String generateMessage(String port, String action, String predecessor1, String successor1, String key, String value) {

        String divider = ":";
        StringBuilder sb = new StringBuilder();
        sb.append(port).append(divider);
        sb.append(action).append(divider);
        sb.append(predecessor1).append(divider).append(successor1).append(divider);
        sb.append(key).append(divider).append(value).append(divider);
        return sb.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private String getPredecessorAndSuccessor(String hashedPort) {
            String predecessor1, successor1;
            if (avdList.higherEntry(hashedPort) == null) {
                successor1 = avdList.firstKey();
            } else {
                successor1 = avdList.higherKey(hashedPort);
            }

            if (avdList.lowerEntry(hashedPort) == null) {
                predecessor1 = avdList.lastKey();
            } else {
                predecessor1 = avdList.lowerKey(hashedPort);
            }
            Log.v(SERVER, "Pred = "+avdList.get(predecessor1) + " Succ = " + avdList.get(successor1));
            return avdList.get(predecessor1) + ":" + avdList.get(successor1);
        }

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            String serverMessage = null;
            ContentValues val = new ContentValues();
            try {
                while (true) {

                    Socket socket = serverSocket.accept();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    serverMessage=reader.readLine();
                    String msg[] = serverMessage.split(":");
                    String portReceived = msg[0];
                    String action = msg[1];
                    String predecessor1 = msg[2];
                    String successor1 = msg[3];
                    String keyReceived = msg[4];
                    String valueReceived = msg[5];
                    Log.v(SERVER, "Port received " + portReceived);
                    Log.v(SERVER, "Action "+ action);
                    Log.v(SERVER, "Predecessor " + predecessor1);

                    String portHashed = genHash(msg[0]);
                    if (action.equalsIgnoreCase("request")) {
                        avdList.put(portHashed, msg[0]);
                        String res = getPredecessorAndSuccessor(portHashed);
                        String s[] = res.split(":");
                        String messageRequest = generateMessage(portReceived,"PreSucc", s[0], s[1], key, value);
                        publishProgress(messageRequest);

                    } else if (action.equalsIgnoreCase("pre")) {
                        successor = successor1;
                        try {
                            hashedSuccessor = genHash(successor);
                            avdList.put(hashedSuccessor, successor);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                    } else if (action.equalsIgnoreCase("succ")) {
                        predecessor = predecessor1;
                        try {
                            hashedPredecessor = genHash(predecessor);
                            avdList.put(hashedPredecessor, predecessor);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                    }
                    else if (action.equalsIgnoreCase("PreSucc")) {
                        predecessor = predecessor1;
                        successor = successor1;
                        try {
                            hashedSuccessor = genHash(successor);
                            hashedPredecessor = genHash(predecessor);
                            avdList.put(hashedSuccessor, successor);
                            avdList.put(hashedPredecessor, predecessor);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                    }
                    else if (action.equalsIgnoreCase("send")) {
                        val.put("key", keyReceived);
                        val.put("value", valueReceived);
                        Uri uri = providerUri;
                        insert(uri, val);
                    }else if (action.equalsIgnoreCase("helpThePoorGuy")) {
                        Cursor cursor = query(providerUri, null, keyReceived, null, null);
                        Log.v("QUERY", "Received value");
                        int valueIndex = cursor.getColumnIndex("value");
                        cursor.moveToFirst();
                        String returnValue = cursor.getString(valueIndex);
                        Log.v("QUERY", "value "+returnValue);
                        PrintWriter pw = new PrintWriter(socket.getOutputStream());
                        pw.println(returnValue);
                        pw.flush();
                        pw.close();
                    } else if (action.equalsIgnoreCase("queryAll")) {
                        Log.v(QUERY, "Request received for * from " + successor1);
                        Cursor cursor = query(providerUri, null, "*", new String[]{successor1}, null);

                        /*
                           Reference:
                            https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                        */
                        StringBuilder sb = new StringBuilder();
                        for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                            int keyIndex = cursor.getColumnIndex("key");
                            int valueIndex = cursor.getColumnIndex("value");
                            String returnKey = cursor.getString(keyIndex);
                            String returnValue = cursor.getString(valueIndex);
                            sb.append(returnKey).append(":").append(returnValue).append("\n");
                            Log.v(QUERY, "Works key "+ returnKey +" : " + returnValue);
                        }

                        PrintWriter pw = new PrintWriter(socket.getOutputStream());
                        pw.println(sb.toString());
                        pw.flush();
                        pw.close();
                        cursor.close();
                    }
                    socket.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {


            String hello[] = strings[0].split(":");
            if (hello[1].equalsIgnoreCase("PreSucc")) {
                String messageS = generateMessage(hello[2], "pre", "c", hello[0], " ", " ");
                String messageP = generateMessage(hello[3], "succ", hello[0], "c", " ", " ");
                ArrayList<String> ar = new ArrayList<String>();
                ar.add(messageS);
                ar.add(messageP);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, strings[0]);
                for (String ii:  ar )
                {
                    new PreSuccTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, ii);

                }


            }
        }
    }
    /*
                        //Reference: https://docs.oracle.com/javase/7/docs/api/java/io/Writer.html
                        //Reference: https://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html
                        //Note: Flushes the stream
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        protected Void doInBackground(String... msgs) {

            String clientMessage[] = msgs[0].split(":");

            try {

                if (clientMessage[1].equalsIgnoreCase("Addme")) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt("11108"));
                    String messageAddme = generateMessage(clientMessage[0], "request", predecessor, successor, key, value);

                    PrintWriter writer = new PrintWriter(socket.getOutputStream());
                    writer.println(messageAddme);
                    writer.flush();
                    System.out.print(avdList.size());
                } else if (clientMessage[1].equalsIgnoreCase("PreSucc")) {
                    int port1 = Integer.parseInt(clientMessage[0]) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port1);

                    String messageSP = msgs[0];

                    PrintWriter writer = new PrintWriter(socket.getOutputStream());
                    writer.println(messageSP);
                    writer.flush();

                } else if (clientMessage[1].equalsIgnoreCase("send")) {
                    int port=Integer.parseInt(successor) * 2;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            port);
                    String storeMessage = msgs[0];

                    PrintWriter writer = new PrintWriter(socket.getOutputStream());
                    writer.println(storeMessage);
                    writer.flush();
                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class PreSuccTask extends AsyncTask<String, Void, Void> {

        protected Void doInBackground(String... msgs) {

            String presucMessage[] = msgs[0].split(":");
            try {
                int port = Integer.parseInt(presucMessage[0]) * 2;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        port);
                String storeMessage = msgs[0];
                PrintWriter writer = new PrintWriter(socket.getOutputStream());
                writer.println(storeMessage);
                writer.flush();

            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}