import tkinter as tk

root = tk.Tk()
text_area = tk.Text(root, height=10, width=40) # Size in lines/chars
text_area.pack()
text_area.insert(tk.END, "Hello World") # Add default text
root.mainloop()
