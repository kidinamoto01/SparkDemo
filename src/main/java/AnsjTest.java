import org.ansj.domain.Result;
import org.ansj.library.UserDefineLibrary;
import org.ansj.recognition.impl.FilterRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;


/**
 * Created by b on 17/3/6.
 */
public class AnsjTest {
    public static void main(String[] args) {
        // 习近平 习近平 nr
        // 李民工作 李民 nr 工作 vn
        // 三个和尚 三个 m 和尚 n
        // 的确定不 的确 d 定 v 不 v
        // 大和尚 大 a 和尚 n
        // 张三和 张三 nr 和 c
        // 动漫游戏 动漫 n 游戏 n
        // 邓颖超生前 邓颖超 nr 生前 t
//		System.out.println(ToAnalysis.parse("学习近平和李克强将称为一种时尚!"));

		System.out.println(ToAnalysis.parse("李民工作了一天!"));
//		System.out.println(ToAnalysis.parse("三个和尚抬水喝!"));
//		System.out.println(ToAnalysis.parse("我想说,这事的确定不下来,我得想!"));
//		System.out.println(ToAnalysis.parse("小和尚剃了一个和大和尚一样的和尚头"));
//		System.out.println(ToAnalysis.parse("我喜欢玩动漫游戏"));
//		System.out.println(ToAnalysis.parse("邓颖超生前最喜欢的一个"));

        System.out.println(ToAnalysis.parse("365亿个日日夜夜"));
        UserDefineLibrary.insertWord("城中村", "userDefine", 1000);//自定义词汇、自定义词性
        UserDefineLibrary.insertWord("ansj中文分词", "userDefine", 1001);
        UserDefineLibrary.removeWord("的");
        //StopLibrary.insertStopWords(1,"ansj中文分词", "userDefine", 1000);
        Result terms = ToAnalysis.parse("我觉得Ansj中文分词是一个不错的系统!我是王婆!");
        System.out.println("增加新词例子:" + terms);
        // 删除词语,只能删除.用户自定义的词典.
        UserDefineLibrary.removeWord("ansj中文分词");
        FilterRecognition fitler = new FilterRecognition();
        //http://nlpchina.github.io/ansj_seg/content.html?name=词性说明
        fitler.insertStopNatures("null"); //过滤标点符号词性
        fitler.insertStopNatures("w");
        terms = ToAnalysis.parse("[28] See William Rubin: Pollock as Jungian Illustrator:The Lim-its of Psychological Criticism,Art in America,November 197\n" +
                "9.\n" +
                "    [29]  See M.Merleau-porty: Sense and Nonsense.Northwestern Un-iversity Press,1964,P.55.").recognition(fitler);
        System.out.println("删除用户自定义词典例子:" + terms);
    }
}
