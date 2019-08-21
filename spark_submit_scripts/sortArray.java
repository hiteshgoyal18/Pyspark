public class sortArray 
{
    public static void main(String[] args) 
    {
        int[] array = {12, 15, 11, 13, 9, 25};
        for (int i = 0; i < array.length - 1; i++) 
        {
            if (array[i] > array[i + 1]) 
            {
                int temp = array[i];
                array[i] = array[i + 1];
                array[i + 1] = temp;
            }
        }
		for(int i=0;i<array.length;i++){
			
		System.out.print(array[i]);
    }}
}