inputlist = ['first_snake_word', 'second_snake_word', 'third_snake_word', 'and_the_list_goes_on']

for eachword in inputlist:
    string1 = eachword
    string2 = string1.split("_")
    outputcamelstring = ''
    for eachpart in string2:
        outputcamelstring += eachpart.title()
    print(outputcamelstring)