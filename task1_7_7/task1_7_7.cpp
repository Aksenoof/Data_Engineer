//7. Написать программу, которая выводит таблицу значений функции y=-2 * x^2 - 5 * x - 8 
//в диапазоне от –4 до +4, с шагом 0,5

#include<iostream>

using namespace std;

int main()
{
    double y=0;
    double x=-4;
    
    while(x<=4)
    {
        y = -2*x*x-5*x-8;
        cout<<x<<"||"<<y<<endl;
        x+=0.5;
    } 
    return 0;
}
