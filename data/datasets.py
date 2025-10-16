import os
import cv2
import torch
from torch.utils.data import Dataset

class SignLanguageDataset(Dataset):
    """
    A PyTorch Dataset class for loading sign language videos and their corresponding labels.

    Attributes:
        data_dir (str): Path to the directory containing the video files.
        num_frames (int): The number of frames to extract from each video clip. Defaults to 13.
        frame_step (int): The step size to sample frames between clips. Defaults to 4.
        transform (callable, optional): A function/transform to apply to the video data.
    
    Methods:
        __len__():
            Returns the total number of video files in the dataset.

        __getitem__(idx):
            Retrieves a single video clip and its corresponding label by index.

        load_video(video_path):
            Loads a video file, extracts the specified number of frames per clip, and converts them to a PyTorch tensor.
    
    Usage Example:
        dataset = SignLanguageDataset(data_dir="path/to/videos", num_frames=13, frame_step=4, transform=some_transform)
        video, label = dataset[0]  # Get the first video clip and its label.
    """

    def __init__(self, data_dir, num_frames=13, frame_step=4, transform=None):
        """
        Initializes the dataset.

        Args:
            data_dir (str): Path to the directory containing the video files.
            num_frames (int): The number of frames to extract from each video clip.
            frame_step (int): The step size to sample frames between clips.
            transform (callable, optional): A function/transform to apply to the video data.
        """
        self.data_dir = data_dir
        self.num_frames = num_frames
        self.frame_step = frame_step
        self.transform = transform
        self.data = [f for f in os.listdir(data_dir) if f.endswith(".mp4")]

    def __len__(self):
        """
        Returns the total number of video files in the dataset.

        Returns:
            int: Number of videos in the dataset.
        """
        return len(self.data)

    def __getitem__(self, idx):
        """
        Retrieves a single video clip and its corresponding label by index.

        Args:
            idx (int): Index of the video.

        Returns:
            tuple: A tensor representing the video clip (TxCxHxW) and its label.
        """
        video_path = os.path.join(self.data_dir, self.data[idx])
        video = self.load_video(video_path)
        label = os.path.splitext(self.data[idx])[0]  # Use the filename (without extension) as the label

        if self.transform:
            video = self.transform(video)

        return video, label

    def load_video(self, video_path):
        """
        Loads a video file, extracts the specified number of frames per clip, and converts them to a PyTorch tensor.

        Args:
            video_path (str): Path to the video file.

        Returns:
            torch.Tensor: A tensor containing the video frames (TxCxHxW) or None if the video cannot be read.
        """
        cap = cv2.VideoCapture(video_path)
        frames = []
        frame_count = 0

        while len(frames) < self.num_frames:
            ret, frame = cap.read()
            if not ret:
                break

            # Only take frames based on the step size
            if frame_count % self.frame_step == 0:
                frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)  # Convert from BGR to RGB
                frame = torch.tensor(frame).permute(2, 0, 1)  # Convert HxWxC to CxHxW
                frames.append(frame.float())  # Ensure tensor is of type float

            frame_count += 1

        cap.release()

        # Ensure correct dimensions even if not enough frames
        if len(frames) < self.num_frames:
            for _ in range(self.num_frames - len(frames)):
                frames.append(torch.zeros_like(frames[-1]))

        return torch.stack(frames)  # Combine frames into a tensor of shape TxCxHxW
                                    # T: Number of frames, C: Number of channels, H: Height, W: Width

