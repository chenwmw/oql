package com.zuora.zan.reportbuilder.orm;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation annotates query methods of Accessor classes and provide
 * "additional" filtering based on the result of another method, which is
 * usually annotated Query. Example:
 * 
 * <pre>
 * <code>
 * public interface MyDAO {
 *   @Query("select * from my_obj where partition_key = :pk"); 
 *   Result&lt;MyObj&gt; getObjByPk(@Param("pk")String pk);
 *   
 *   @Source("getObjByPk")
 *   @Filter("name like :name and not deleted")
 *   Result&lt;MyObj&gt; getObjLike(@Param("pk")String pk, @Param("name")String name); 
 *   
 *   @Source("getObjByPk")
 *   @Filter("age &gt; :age and not single")
 *   Iterable&lt;MyObj&gt; getObjSingle(@Param("pk")String pk, @Param("age")int age);
 * }
 * </code>
 * </pre>
 * 
 * "getObjLike" will first retrieve objects with partition_key "pk" and then
 * filter the objects returned with a specified name. This method also filters
 * out objects which are softly deleted by setting "deleted" to true.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Filter {
	/**
	 * The filtering expression. By default, it is "not deleted".
	 * 
	 * @return
	 */
	String value();
}
