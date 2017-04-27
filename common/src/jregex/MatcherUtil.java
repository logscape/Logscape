package jregex;

import java.lang.reflect.Field;

/**
 * This is a lovely hack into java so we can prevent thrashing the stack when pulling out jregexp groups - we dont want group-0
 * @author Neiil
 *
 */
public class MatcherUtil {
	static Field memRegField = null;


    static {
        try {
            memRegField = Matcher.class.getDeclaredField("memregs");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        memRegField.setAccessible(true);

    }
//	static Method getStringMethod = null;
	
	/**
	 * Replaces matcher.groups() - same but no group=0
	 * @param matcher
	 * @param nextLine 
	 * @return
	 */
	public static String[] groups(Matcher matcher, String nextLine) {
		try {
//            if (memRegField == null) {
//                synchronized (lock) {
//                    if (memRegField == null) {
//                         memRegField = Matcher.class.getDeclaredField("memregs");
//                        memRegField.setAccessible(true);
//    //					 getStringMethod = Matcher.class.getDeclaredMethod("getString", int.class, int.class);
//    //					getStringMethod.setAccessible(true);
//                    }
//                }
//            }
			
			MemReg[] memregs = (MemReg[]) memRegField.get(matcher);
			String[] groups=new String[memregs.length-1];
		      int in,out;
		      MemReg mr;
		      for(int i=1;i<memregs.length;i++){
		         in=(mr=memregs[i]).in;
		         out=mr.out;
		         if((in=mr.in)<0 || mr.out<in) continue;
//		         groups[i-1]=(String) getStringMethod.invoke(matcher, in,out);
		         groups[i-1]= nextLine.substring(in, out);
		      }
		      return groups;
			
			
		} catch (Exception e) {
			System.err.println("ERROR - Check ThreadContext because the follow error happens when a Matcher is shared between threads");
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
	}
}
