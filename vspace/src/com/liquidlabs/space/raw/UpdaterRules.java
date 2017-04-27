package com.liquidlabs.space.raw;

import com.liquidlabs.common.collection.Arrays;

/**
 * Usage <br>
 * - x|x|replace:YY<br>
 * - x|x|concat:YY<br>
 * - x|x|prepend:YY<br>
 * - x|x|replace:YY<br>
 * - x|x|*=:0.2<br>
 * - x|x|/=:0.2<br>
 * - x|x|-=:0.2<br>
 * - x|x|+=:0.2<br>
 * 
 * @author neil
 *
 */
public class UpdaterRules {
	private static final String TOKEN = ":";
	public Updater[] rules = new Updater[] {
			new Concat("concat:"),
			new Prepend("prepend:"),
			new Replace("replace:"),
			new Replace("replaceWith:"),
			new Multiply("*=:"),
			new Divide("/=:"),
			new Subtract("-=:"),
			new Plus("+=:")
	};
	public static class Concat extends UpdaterBase {

		public Concat(String signature) {
			super(signature);
		}

		public String update(String arg1, String arg2) {
			return arg1 + arg2.substring(arg2.indexOf(TOKEN)+1);
		}
	}
	public static class Prepend extends UpdaterBase{
		public Prepend(String signature) {
			super(signature);
		}
		public String update(String arg1, String arg2) {
			return arg2.substring(arg2.indexOf(TOKEN)+1) + arg1;
		}
	}
	public static class Replace extends UpdaterBase {
		public Replace(String signature) {
			super(signature);
		}
		public String update(String arg1, String arg2) {
			return arg2.substring(arg2.indexOf(TOKEN)+1);
		}
	}
	public static class Plus extends UpdaterBase {
		public Plus(String signature) {
			super(signature);
		}
		public String update(String arg1, String arg2) {
			Double int1 = Double.parseDouble(arg1);
			Double int2 = Double.parseDouble(Arrays.split(TOKEN, arg2)[1]);
			return stripDecimalPlace(new Double(int1 + int2).toString());
		}
	}
	public static class Multiply extends UpdaterBase {
		public Multiply(String signature) {
			super(signature);
		}
		public String update(String arg1, String arg2) {
			Double int1 = Double.parseDouble(arg1);
			Double int2 = Double.parseDouble(Arrays.split(TOKEN, arg2)[1]);
			return stripDecimalPlace(new Double(int1 * int2).toString());
		}
	}
	
	public static class Divide extends UpdaterBase {
		public Divide(String signature) {
			super(signature);
		}
		public String update(String arg1, String arg2) {
			Double int1 = Double.parseDouble(arg1);
			Double int2 = Double.parseDouble(Arrays.split(TOKEN, arg2)[1]);
			return stripDecimalPlace(new Double(int1 / int2).toString());
		}
	}
	
	public static class Subtract extends UpdaterBase {
		public Subtract(String signature) {
			super(signature);
		}
		public String update(String arg1, String arg2) {
			Double int1 = Double.parseDouble(arg1);
			Double int2 = Double.parseDouble(Arrays.split(TOKEN, arg2)[1]);
			return stripDecimalPlace(new Double(int1 - int2).toString());
		}
	}
	
	public abstract static class UpdaterBase implements Updater {
		private final String signature;

		public UpdaterBase(String signature) {
			this.signature = signature;
		}
		public boolean isApplicable(String type) {
			return type.contains(signature);
		}
		String stripDecimalPlace(String value){
			if (value.endsWith(".0")) return Arrays.split(".", value)[0];
			else return value;
		}
	}
}
