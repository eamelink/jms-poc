public class MessageProtocol {
    public String handleProtocolMessage(String messageText) {
        String responseText;
        if(messageText.substring(0, 1).equals("x")) {
          try {
            Thread.sleep(250);
          } catch(java.lang.InterruptedException e) {
            // Do nuttin'
          }
        }
        responseText = messageText + " bar!";

        return responseText;
    }
}
