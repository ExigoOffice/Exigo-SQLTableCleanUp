namespace SQLTableCleanUp
{
    partial class CleanUp
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.cleanupBtn = new System.Windows.Forms.Button();
            this.outputBox = new System.Windows.Forms.RichTextBox();
            this.clearBtn = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // cleanupBtn
            // 
            this.cleanupBtn.Location = new System.Drawing.Point(47, 24);
            this.cleanupBtn.Name = "cleanupBtn";
            this.cleanupBtn.Size = new System.Drawing.Size(165, 41);
            this.cleanupBtn.TabIndex = 0;
            this.cleanupBtn.Text = "Run Clean-up";
            this.cleanupBtn.UseVisualStyleBackColor = true;
            this.cleanupBtn.Click += new System.EventHandler(this.cleanupBtn_Click);
            // 
            // outputBox
            // 
            this.outputBox.Location = new System.Drawing.Point(47, 98);
            this.outputBox.Name = "outputBox";
            this.outputBox.Size = new System.Drawing.Size(584, 271);
            this.outputBox.TabIndex = 1;
            this.outputBox.Text = "";
            // 
            // clearBtn
            // 
            this.clearBtn.Location = new System.Drawing.Point(466, 24);
            this.clearBtn.Name = "clearBtn";
            this.clearBtn.Size = new System.Drawing.Size(165, 41);
            this.clearBtn.TabIndex = 2;
            this.clearBtn.Text = "Clear Output";
            this.clearBtn.UseVisualStyleBackColor = true;
            this.clearBtn.Click += new System.EventHandler(this.clearBtn_Click);
            // 
            // CleanUp
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(691, 428);
            this.Controls.Add(this.clearBtn);
            this.Controls.Add(this.outputBox);
            this.Controls.Add(this.cleanupBtn);
            this.Name = "CleanUp";
            this.Text = "SQL Table Clean-up Utility";
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button cleanupBtn;
        private System.Windows.Forms.RichTextBox outputBox;
        private System.Windows.Forms.Button clearBtn;
    }
}

