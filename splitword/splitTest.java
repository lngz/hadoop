import java.io.*;//


public class splitTest  
{  
    
    /*
     * �ָ��õ�|����java����ô��ʾ�أ�
     * ���ǣ� \\|
     * 
     * */
    
    public static void main(String[] args)  
    {  
        String str=null;//
        BufferedReader stdIn=new BufferedReader(new InputStreamReader(System.in));
        try//
        {
            str=stdIn.readLine();//
            System.out.println(str);
            String dnsCacheLog[] = str.split("\\|");  
            System.out.println(dnsCacheLog[0]);  
            System.out.println(dnsCacheLog[1]);  
            System.out.println(dnsCacheLog[2]);  
            System.out.println(dnsCacheLog[3]);  
        }
        catch(IOException e)
        {
            System.out.println("IO Error!");
        }
        
        
 
        
        
    }  
}  
