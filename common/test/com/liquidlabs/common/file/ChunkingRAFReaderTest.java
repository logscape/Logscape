package com.liquidlabs.common.file;

import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;

import com.liquidlabs.common.file.raf.ChunkingRAFReader;
import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

public class ChunkingRAFReaderTest {
	
	String[] lines = new String[] {
			"         -\n"
	
//			" 2010/09/02 12:59:59| 0- feuh|debug|trdc01 |req|/cmadj/q1.nydailynews/home;kw=additional_discounttires_q3_rosaustin_multiple_300x250;sz=300x250;ord=1283450399305050;env=ifr;ord1=9877;cmpgurl=http%253A//www.nydailynews.com/gossip/galleries/celebs_worst_photos/celebs_worst_photos.html|http://ad.doubleclick.net/adj/q1.nydailynews/home;net=q1;u=,q1-66623892_1283450399,1195cb59f08b75b,ent,cm.games-cm.games_L-cm.music_L-cm.ent_L-cm.fin_L-ti.174-ti.221-ti.71-an.26-ex.4l-ex.4m-ex.1u-ex.fm-ex.dw-ex.ap;;kw=additional_discounttires_q3_rosaustin_multiple_300x250;env=ifr;dc=w;ord1=9877;sz=300x250;contx=ent;btg=cm.games;btg=cm.games_L;btg=cm.music_L;btg=cm.ent_L;btg=cm.fin_L;btg=ti.174;btg=ti.221;btg=ti.71;btg=an.26;btg=ex.4l;btg=ex.4m;btg=ex.1u;btg=ex.fm;btg=ex.dw;btg=ex.ap;ord=1283450399305050?rt=6|0.003739|E|\n", 
//			" 2010/09/02 12:59:59|1-feuh|debug|trdc01 |req|/adj/ns.productreviews/gaming;ppos=ATF;kw=;tile=1;dcopt=ist;sz=728x90;ord=5120901554267361||0.000399|E|\n", 
//			" 2010/09/02 12:59:59|2-feuh|debug|trdc01 |req|/adj/idgt.osdir/article_above;sec=article;fold=above;tile=2;sz=300x250;ord=3496826622416961||0.000479|E|\n", 
//			" 2010/09/02 12:59:59|3-feuh|debug|trdc01 |req|/adj/cdnl.telegraaf_admeld/716977;sz=336x280;ord=1283450399||0.000427|E|\n", 
//			" 2010/09/02 12:59:59|4-feuh|debug|trdc01 |req|/adj/n1175.entertainment/games_content;pos=mpu_bottom;topic=null;publisher=games;source=arkadium;grab=all;cmn=aarp;adv_accept=commercial;pageid=/content/aarp/en/home/entertainment/games/info-01-2010;dir_index=no;logged_in=true;member=status-0;mem_exp=null;author=null;language=english;site=n1175.entertainment;zone=games_content;package=null;profileID=memere91;gamename=mahjongg-dimensions-stouffers;gamecat=mahjongg-dimensions-stouffers;gamepgtype=description;kw=mah+jong;sz=300x250,300x600,160x600;tile=4;ord=869552955680406||0.00065|E|\n", 
//			" 2010/09/02 12:59:59|5-feuh|debug|trdc01 |req|/adj/idgt.redorbit/article_above;sec=article;fold=above;tile=1;dcopt=ist;sz=728x90;ord=3683161794558241||0.000371|E|\n", 
//			" 2010/09/02 12:59:59|6-feuh|debug|trdc01 |req|/cmadj/cm.earthlink/;sz=728x90;ord=2587851;env=ifr;ord1=126776;cmpgurl=http%253A//webmail.earthlink.net/wam/msg.jsp%253Fmsgid%253D19165%2526folder%253DINBOX%2526isSeen%253Dfalse%2526x%253D1482061805|http://ad.doubleclick.net/adj/cm.earthlink/;net=cm;u=,cm-54223731_1283450399,11a81992ca882fe,Miscellaneous,;;env=ifr;dc=w;ord1=126776;sz=728x90;contx=Miscellaneous;btg=;ord=2587851?rt=6|0.010712|E|\n", 
//			" 2010/09/02 12:59:59|7-feuh|debug|trdc01 |req|/cmadj/cdde.admention/930935;sz=160x600;ord=%5Btimestamp%5D;env=ifr;ord1=659780;cmpgurl=http%253A//getvids.de/index.php|http://ads.creative-serving.com/adj/cdde.admention/930935;net=cdde;u=,cdde-19509202_1283450399,11ac7b923c8041a,de_technologie,cdde.de_technologie_H-cdde.de_gadgets_H-cdde.de_entertainment_H-cdde.de_spiele_H-cdde.de_shopping_H-cdde.de_wissenschaft_H-cdde.de_business_M;;env=ifr;dc=w;ord1=659780;sz=160x600;contx=de_technologie;btg=cdde.de_technologie_H;btg=cdde.de_gadgets_H;btg=cdde.de_entertainment_H;btg=cdde.de_spiele_H;btg=cdde.de_shopping_H;btg=cdde.de_wissenschaft_H;btg=cdde.de_business_M;ord=%5Btimestamp%5D?rt=4|0.003885|E|\n", 
//			" 2010/09/02 12:59:59|8-feuh|debug|trdc01 |req|/adj/cm.gannett/;sz=160x600;ord=[timestamp]||0.000488|C|\n", 
//			" 2010/09/02 12:59:59|9-feuh|debug|trdc01 |req|/cmadj/cm.jump_1/;sz=160x600;ord=[timestamp];ord1=174963;cmpgurl=http%253A//www.gobison.com/SportSelect.dbml%253F%2526DB_OEM_ID%253D2400%2526SPID%253D695%2526SPSID%253D11850|http://ad.doubleclick.net/adj/cm.jump_1/;net=cm;u=,cm-6905546_1283450399,1193cb014b83e66,sports,cm.nfl_M-cm.drudge-cm.tech-cm.tech_L-cm.baseball_M-cm.sports_L-cm.polit_L-cm.shop-cm.shop_M-cm.basketb_L-cm.sportsreg-cm.health_L-mm.ac5-mm.ag1-mm.ak5-mm.am1-mm.as5-an.109-wfm.difi_M;;dc=w;ord1=174963;sz=160x600;contx=sports;btg=cm.nfl_M;btg=cm.drudge;btg=cm.tech;btg=cm.tech_L;btg=cm.baseball_M;btg=cm.sports_L;btg=cm.polit_L;btg=cm.shop;btg=cm.shop_M;btg=cm.basketb_L;btg=cm.sportsreg;btg=cm.health_L;btg=mm.ac5;btg=mm.ag1;btg=mm.ak5;btg=mm.am1;btg=mm.as5;btg=an.109;btg=wfm.difi_M;ord=[timestamp]?rt=6|0.00453|E|\n" 
	}; 
	String aLine = " 2010/09/02 12:59:59|9-feuh|debug|trdc01 |req|/cmadj/cm.jump_1/;sz=160x600;ord=[timestamp];ord1=174963;cmpgurl=http%253A//www.gobison.com/SportSelect.dbml%253F%2526DB_OEM_ID%253D2400%2526SPID%253D695%2526SPSID%253D11850|http://ad.doubleclick.net/adj/cm.jump_1/;net=cm;u=,cm-6905546_1283450399,1193cb014b83e66,sports,cm.nfl_M-cm.drudge-cm.tech-cm.tech_L-cm.baseball_M-cm.sports_L-cm.polit_L-cm.shop-cm.shop_M-cm.basketb_L-cm.sportsreg-cm.health_L-mm.ac5-mm.ag1-mm.ak5-mm.am1-mm.as5-an.109-wfm.difi_M;;dc=w;ord1=174963;sz=160x600;contx=sports;btg=cm.nfl_M;btg=cm.drudge;btg=cm.tech;btg=cm.tech_L;btg=cm.baseball_M;btg=cm.sports_L;btg=cm.polit_L;btg=cm.shop;btg=cm.shop_M;btg=cm.basketb_L;btg=cm.sportsreg;btg=cm.health_L;btg=mm.ac5;btg=mm.ag1;btg=mm.ak5;btg=mm.am1;btg=mm.as5;btg=an.109;btg=wfm.difi_M;ord=[timestamp]?rt=6|0.00453|E|"; 
			
	
	
	long start = DateTimeUtils.currentTimeMillis();
	long end = DateTimeUtils.currentTimeMillis();
	int lineCount = 0;
	
	int linesWritten = 0;
	@Test
	public void shouldNotReadEmptyLinesOnLiveFile() throws Exception {
		final String filename = "build/chunkPrintWriter.log";
		final File file = new File(filename);
		if (file.exists()) file.delete();
        file.getParentFile().mkdirs();
        final PrintWriter pw = new PrintWriter(file);
		

		
		Thread thread = new Thread() {
			public void run() {
				try {
					while (true) {
						
						pw.println(linesWritten++);
						pw.flush();
						Thread.sleep(500);
					}
				} catch (InterruptedException e) {
				}
			}
		};
		thread.start();
		Thread.sleep(2000);
		try {
			ChunkingRAFReader rafChunk = new ChunkingRAFReader(file.getAbsolutePath(),"");
			List<String> lines = rafChunk.readLines(0);
			
			for (String string : lines) {
				assertTrue(string.length() > 0);
			}
		} finally {
			thread.interrupt();
		}
		
		
	}
	
	@Test
	public void shouldReadBigFileFast() throws Exception {
		
		File file = writeFileContents();
		
		performMLineReaderPerformanceTst(file);
		
		ChunkingRAFReader rafChunk = new ChunkingRAFReader(file.getAbsolutePath(), "");
		List<String> chunk = null;
		long pos = 0;
		int lastLineRead = -1;
		boolean validateLineSequence = false;
		
		while ( (chunk = rafChunk.readLines(pos)) != null) {
			pos = rafChunk.getFilePointer();
			lineCount += chunk.size();
			if (validateLineSequence) {
				for (String string : chunk) {
					String aString = string.split(" ")[0];
					int parseInt = Integer.parseInt(aString);
					if (parseInt != lastLineRead +1) {
						System.out.println("LAST:" + lastLineRead + "  >>***** ERROR ********** :" + string);
					}
					lastLineRead = parseInt;
				}
			}
		}
		msg(">>> CHUNK READ Complete Lines:" + lineCount);
		
	}

	private File writeFileContents() throws FileNotFoundException, IOException {
		int numberOfLines = 5 * 10;
		msg("Going to write line count:" + (numberOfLines));
		File file = new File("build", "ChunkLogFileTest.log");
		if (file.exists()) file.delete();
		
		OutputStream fos = new BufferedOutputStream(new FileOutputStream(file));
		int i = 0;
		while (i < numberOfLines) {
			for (String lineItem : lines) {
//				fos.write((formatFixedWidth(i, 9) + aLine +  "\n").getBytes());	
				String string = formatFixedWidth(i, 9) + aLine;
//				fos.write((string.substring(0, 199) + "\n").getBytes());	
				fos.write((string + "\n").getBytes());	
				i++;
			}
		}
		fos.close();
		msg("Write Complete");
		return file;
	}

	private void performMLineReaderPerformanceTst(File file) throws FileNotFoundException, IOException {
		RAF raf = RafFactory.getRaf(file.getAbsolutePath(), "");
		String line = null;
		while ((line = raf.readLine()) != null) {
			lineCount++;
		}
		msg(">>> RAF READ Complete lines:" + lineCount);
	}

	private void msg(String string) {
		end = DateTimeUtils.currentTimeMillis();
		System.out.println(new Date().toString() + " " + string + " elapsedMs:" + (end - start));
		start = DateTimeUtils.currentTimeMillis();
		lineCount = 0;
		
	}
	  public static String formatFixedWidth(int value, int width) {
	        char[] chars;
	        int i;
	        int c;

	        chars = new char[width];
	        for (i = 0; i < width; i++) {
	            c = value % 10;
	            chars[width-i-1] = (char)('0' + c);
	            value = value / 10;
	        }
	        return new String(chars);
	    }


}
