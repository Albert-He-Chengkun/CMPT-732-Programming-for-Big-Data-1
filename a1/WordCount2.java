public static class FloatLineMapper
extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException {
		// TODO: write code to go here
		String[] nums = value.toString().split(",");
      	double sum = 0.0;
      	for (String num: nums){
          double floatValue = Double.parseDouble(num.trim());
          sum += floatValue;
        }
      	context.write(new Text("sum"), new DoubleWritable(sum));
	}
}
// ...
job.setMapperClass(FloatLineMapper.class);
job.setCombinerClass(DoubleSumReducer.class);
job.setReducerClass(DoubleSumReducer.class);